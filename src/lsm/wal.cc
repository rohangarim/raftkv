#include "lsm/wal.h"

#include <cerrno>
#include <cstring>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

#include "lsm/coding.h"
#include "lsm/crc32c.h"
#include "lsm/file.h"

namespace raftkv::lsm {
namespace {

constexpr size_t kHeaderSize = 8;  // crc32 + length

}  // namespace

WalWriter::WalWriter(std::unique_ptr<WritableFile> file) : file_(std::move(file)) {}

WalWriter::~WalWriter() = default;

Result<std::unique_ptr<WalWriter>> WalWriter::Open(const std::filesystem::path& path) {
  auto opened = WritableFile::OpenForAppend(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<WalWriter> writer(new WalWriter(opened.TakeValue()));
  return writer;
}

Status WalWriter::Append(std::string_view payload) {
  if (file_ == nullptr) {
    return Status::IoError("append to closed WAL");
  }
  if (payload.size() > 0xFFFFFFFFULL) {
    return Status::InvalidArgument("WAL record exceeds 4 GiB");
  }

  // The CRC covers the length field as well as the payload. Covering only the
  // payload would let a corrupted length silently reframe every record after
  // it, and every one of those CRCs would still check out -- corruption that
  // reads as valid data.
  std::string framed;
  framed.reserve(4 + payload.size());
  PutFixed32(&framed, static_cast<uint32_t>(payload.size()));
  framed.append(payload);

  std::string record;
  record.reserve(kHeaderSize + payload.size());
  PutFixed32(&record, Crc32c(framed.data(), framed.size()));
  record.append(framed);

  bytes_written_ += record.size();
  return file_->Append(record);
}

Status WalWriter::Sync() {
  if (file_ == nullptr) {
    return Status::IoError("sync of closed WAL");
  }
  return file_->Sync();
}

Status WalWriter::Close() {
  if (file_ == nullptr) {
    return Status::Ok();
  }
  const Status s = file_->Close();
  file_.reset();
  return s;
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
