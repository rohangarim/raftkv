#include "lsm/wal.h"

#include <cerrno>
#include <cstring>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

#include "lsm/coding.h"
#include "lsm/crc32c.h"

namespace raftkv::lsm {
namespace {

constexpr size_t kHeaderSize = 8;  // crc32 + length
constexpr size_t kBufferTarget = size_t{64} * 1024;

// strerror() is not thread safe: it may return a pointer to a shared static
// buffer that another thread is concurrently overwriting. In a system whose
// whole point is concurrent replication, a mangled error message is the least
// of it -- this is a genuine data race.
//
// strerror_r is the thread-safe replacement, but it has two incompatible
// signatures in the wild: the XSI version (macOS, musl) returns int and fills
// the caller's buffer, while glibc's GNU version returns char* that may or may
// not point into that buffer. Dispatching on the return type handles both
// without preprocessor guesswork about which libc this is.
// The branch must depend on a template parameter: `if constexpr` still
// type-checks a discarded branch when its condition is not type-dependent,
// so a plain function body would fail to compile on whichever libc it is not
// being built against.
template <typename R>
std::string StrerrorResult(R result, const char* buf, int err) {
  if constexpr (std::is_same_v<R, char*>) {
    return result != nullptr ? std::string(result) : std::string("unknown error");
  } else {
    // XSI: non-zero means the message did not fit, or errno was unrecognized.
    return result == 0 ? std::string(buf) : ("errno " + std::to_string(err));
  }
}

std::string SafeStrerror(int err) {
  char buf[256];
  buf[0] = '\0';
  return StrerrorResult(::strerror_r(err, buf, sizeof(buf)), buf, err);
}

std::string ErrnoMessage(std::string_view what, const std::filesystem::path& path) {
  std::string msg(what);
  msg += " ";
  msg += path.string();
  msg += ": ";
  msg += SafeStrerror(errno);
  return msg;
}

// write(2) is allowed to write fewer bytes than asked, including on regular
// files when a signal arrives. Looping is not paranoia, it is the contract.
Status WriteFully(int fd, const char* data, size_t size, const std::filesystem::path& path) {
  while (size > 0) {
    const ssize_t n = ::write(fd, data, size);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IoError(ErrnoMessage("write", path));
    }
    data += n;
    size -= static_cast<size_t>(n);
  }
  return Status::Ok();
}

}  // namespace

WalWriter::WalWriter(int fd, std::filesystem::path path) : fd_(fd), path_(std::move(path)) {
  buffer_.reserve(kBufferTarget);
}

WalWriter::~WalWriter() {
  // Best effort: a destructor cannot report failure, and a caller who needs
  // durability must call Sync() explicitly.
  if (fd_ >= 0) {
    (void)FlushBuffer();
    ::close(fd_);
    fd_ = -1;
  }
}

Result<std::unique_ptr<WalWriter>> WalWriter::Open(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("open", path));
  }
  std::unique_ptr<WalWriter> writer(new WalWriter(fd, path));
  return writer;
}

Status WalWriter::Append(std::string_view payload) {
  if (fd_ < 0) {
    return Status::IoError("append to closed WAL");
  }
  if (payload.size() > 0xFFFFFFFFULL) {
    return Status::InvalidArgument("WAL record exceeds 4 GiB");
  }

  // CRC covers the length field as well as the payload. Covering only the
  // payload would let a corrupted length silently reframe every record after
  // it, and the CRCs would still check out.
  std::string framed;
  framed.reserve(4 + payload.size());
  PutFixed32(&framed, static_cast<uint32_t>(payload.size()));
  framed.append(payload);

  const uint32_t crc = Crc32c(framed.data(), framed.size());

  const size_t before = buffer_.size();
  PutFixed32(&buffer_, crc);
  buffer_.append(framed);
  bytes_written_ += buffer_.size() - before;

  if (buffer_.size() >= kBufferTarget) {
    return FlushBuffer();
  }
  return Status::Ok();
}

Status WalWriter::FlushBuffer() {
  if (buffer_.empty()) {
    return Status::Ok();
  }
  RAFTKV_RETURN_IF_ERROR(WriteFully(fd_, buffer_.data(), buffer_.size(), path_));
  buffer_.clear();
  return Status::Ok();
}

Status WalWriter::Sync() {
  RAFTKV_RETURN_IF_ERROR(FlushBuffer());
#if defined(__APPLE__)
  // fsync() on macOS flushes to the drive but does not necessarily force the
  // drive's own write cache. F_FULLFSYNC does. Using plain fsync here would
  // make durability claims this code cannot back up.
  if (::fcntl(fd_, F_FULLFSYNC, 0) < 0) {
    return Status::IoError(ErrnoMessage("F_FULLFSYNC", path_));
  }
#else
  if (::fdatasync(fd_) < 0) {
    return Status::IoError(ErrnoMessage("fdatasync", path_));
  }
#endif
  return Status::Ok();
}

Status WalWriter::Close() {
  if (fd_ < 0) {
    return Status::Ok();
  }
  RAFTKV_RETURN_IF_ERROR(FlushBuffer());
  if (::close(fd_) < 0) {
    fd_ = -1;
    return Status::IoError(ErrnoMessage("close", path_));
  }
  fd_ = -1;
  return Status::Ok();
}

WalReader::WalReader(std::string contents) : contents_(std::move(contents)) {}

Result<std::unique_ptr<WalReader>> WalReader::Open(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("open", path));
  }

  std::string contents;
  char chunk[64 * 1024];
  while (true) {
    const ssize_t n = ::read(fd, chunk, sizeof(chunk));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(fd);
      return Status::IoError(ErrnoMessage("read", path));
    }
    if (n == 0) {
      break;
    }
    contents.append(chunk, static_cast<size_t>(n));
  }
  ::close(fd);

  std::unique_ptr<WalReader> reader(new WalReader(std::move(contents)));
  return reader;
}

Status WalReader::ReadRecord(std::string* record) {
  good_offset_ = offset_;

  if (offset_ == contents_.size()) {
    return Status::NotFound("end of WAL");
  }
  if (contents_.size() - offset_ < kHeaderSize) {
    return Status::Corruption("WAL: truncated record header");
  }

  const uint32_t stored_crc = DecodeFixed32(contents_.data() + offset_);
  const uint32_t length = DecodeFixed32(contents_.data() + offset_ + 4);

  if (contents_.size() - offset_ - kHeaderSize < length) {
    return Status::Corruption("WAL: truncated record payload");
  }

  const char* framed = contents_.data() + offset_ + 4;  // length field onward
  const uint32_t actual_crc = Crc32c(framed, 4 + static_cast<size_t>(length));
  if (actual_crc != stored_crc) {
    return Status::Corruption("WAL: record CRC mismatch");
  }

  record->assign(contents_, offset_ + kHeaderSize, length);
  offset_ += kHeaderSize + length;
  good_offset_ = offset_;
  return Status::Ok();
}

Status ReplayWal(const std::filesystem::path& path, bool truncate_partial,
                 std::vector<std::string>* records) {
  records->clear();

  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    return Status::Ok();  // nothing to replay is not an error
  }

  auto opened = WalReader::Open(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<WalReader> reader = opened.TakeValue();

  while (true) {
    std::string record;
    Status s = reader->ReadRecord(&record);
    if (s.IsOk()) {
      records->push_back(std::move(record));
      continue;
    }
    if (s.ErrCode() == Code::kNotFound) {
      return Status::Ok();  // clean EOF
    }

    // A damaged tail is the expected result of a crash mid-append, not a bug.
    // Everything before it is intact and was never acknowledged, so dropping
    // it loses nothing a caller was promised.
    if (!truncate_partial) {
      return s;
    }
    const uint64_t good = reader->TruncateOffset();
    std::filesystem::resize_file(path, good, ec);
    if (ec) {
      return Status::IoError("truncating damaged WAL tail: " + ec.message());
    }
    return Status::Ok();
  }
}

}  // namespace raftkv::lsm
