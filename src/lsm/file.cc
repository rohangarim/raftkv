#include "lsm/file.h"

#include <cerrno>
#include <cstring>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace raftkv::lsm {
namespace {

constexpr size_t kWriteBufferTarget = size_t{64} * 1024;

// The branch must depend on a template parameter: `if constexpr` still
// type-checks a discarded branch when its condition is not type-dependent, so
// a plain function body would fail to compile against whichever libc this is
// not being built for.
template <typename R>
std::string StrerrorResult(R result, const char* buf, int err) {
  if constexpr (std::is_same_v<R, char*>) {
    return result != nullptr ? std::string(result) : std::string("unknown error");
  } else {
    return result == 0 ? std::string(buf) : ("errno " + std::to_string(err));
  }
}

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

Status SyncFd(int fd, const std::filesystem::path& path) {
#if defined(__APPLE__)
  if (::fcntl(fd, F_FULLFSYNC, 0) < 0) {
    return Status::IoError(ErrnoMessage("F_FULLFSYNC", path));
  }
#else
  if (::fdatasync(fd) < 0) {
    return Status::IoError(ErrnoMessage("fdatasync", path));
  }
#endif
  return Status::Ok();
}

}  // namespace

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

// ---------------------------------------------------------------------------
// RandomAccessFile
// ---------------------------------------------------------------------------

RandomAccessFile::RandomAccessFile(int fd, uint64_t size, std::filesystem::path path)
    : fd_(fd), size_(size), path_(std::move(path)) {}

RandomAccessFile::~RandomAccessFile() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Result<std::unique_ptr<RandomAccessFile>> RandomAccessFile::Open(
    const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("open", path));
  }

  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    ::close(fd);
    return Status::IoError("stat " + path.string() + ": " + ec.message());
  }

  std::unique_ptr<RandomAccessFile> file(new RandomAccessFile(fd, size, path));
  return file;
}

Status RandomAccessFile::ReadExactly(uint64_t offset, size_t n, std::string* out) const {
  out->resize(n);
  size_t done = 0;
  while (done < n) {
    const ssize_t got =
        ::pread(fd_, out->data() + done, n - done, static_cast<off_t>(offset + done));
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IoError(ErrnoMessage("pread", path_));
    }
    if (got == 0) {
      return Status::Corruption("short read: file truncated at offset " +
                                std::to_string(offset + done) + " in " + path_.string());
    }
    done += static_cast<size_t>(got);
  }
  return Status::Ok();
}

// ---------------------------------------------------------------------------
// WritableFile
// ---------------------------------------------------------------------------

WritableFile::WritableFile(int fd, std::filesystem::path path) : fd_(fd), path_(std::move(path)) {
  buffer_.reserve(kWriteBufferTarget);
}

WritableFile::~WritableFile() {
  if (fd_ >= 0) {
    (void)Flush();
    ::close(fd_);
    fd_ = -1;
  }
}

Result<std::unique_ptr<WritableFile>> WritableFile::Create(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("create", path));
  }
  std::unique_ptr<WritableFile> file(new WritableFile(fd, path));
  return file;
}

Result<std::unique_ptr<WritableFile>> WritableFile::OpenForAppend(
    const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("open for append", path));
  }
  std::unique_ptr<WritableFile> file(new WritableFile(fd, path));
  return file;
}

Status WritableFile::Append(std::string_view data) {
  if (fd_ < 0) {
    return Status::IoError("append to closed file");
  }
  buffer_.append(data);
  offset_ += data.size();
  if (buffer_.size() >= kWriteBufferTarget) {
    return Flush();
  }
  return Status::Ok();
}

Status WritableFile::Flush() {
  if (buffer_.empty() || fd_ < 0) {
    return Status::Ok();
  }
  RAFTKV_RETURN_IF_ERROR(WriteFully(fd_, buffer_.data(), buffer_.size(), path_));
  buffer_.clear();
  return Status::Ok();
}

Status WritableFile::Sync() {
  RAFTKV_RETURN_IF_ERROR(Flush());
  return SyncFd(fd_, path_);
}

Status WritableFile::Close() {
  if (fd_ < 0) {
    return Status::Ok();
  }
  RAFTKV_RETURN_IF_ERROR(Flush());
  const int fd = fd_;
  fd_ = -1;
  if (::close(fd) < 0) {
    return Status::IoError(ErrnoMessage("close", path_));
  }
  return Status::Ok();
}

Status SyncDirectory(const std::filesystem::path& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IoError(ErrnoMessage("open dir", dir));
  }
  // Directory syncs use fsync, not F_FULLFSYNC: there is no drive write cache
  // to flush for a directory entry, and F_FULLFSYNC on a directory fd is not
  // portable.
  const int rc = ::fsync(fd);
  const int saved = errno;
  ::close(fd);
  if (rc < 0) {
    errno = saved;
    return Status::IoError(ErrnoMessage("fsync dir", dir));
  }
  return Status::Ok();
}

}  // namespace raftkv::lsm
