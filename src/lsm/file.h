#pragma once

// Thin POSIX file wrappers.
//
// pread/pwrite rather than read/write plus lseek, because the file position is
// shared process-wide state: two threads reading the same fd with lseek+read
// interleave and corrupt each other's reads. The engine serves reads
// concurrently, so positional I/O is the only correct choice here.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "lsm/status.h"

namespace raftkv::lsm {

// Formats errno safely. std::strerror shares a static buffer across threads.
std::string SafeStrerror(int err);

std::string ErrnoMessage(std::string_view what, const std::filesystem::path& path);

// Read-only file supporting concurrent positional reads.
class RandomAccessFile {
 public:
  static Result<std::unique_ptr<RandomAccessFile>> Open(const std::filesystem::path& path);
  ~RandomAccessFile();

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  // Reads exactly `n` bytes at `offset`. A short read is an error, not a
  // partial success: every caller here is reading a structure whose length it
  // already knows, so a short read means the file is truncated.
  Status ReadExactly(uint64_t offset, size_t n, std::string* out) const;

  uint64_t Size() const { return size_; }
  const std::filesystem::path& Path() const { return path_; }

 private:
  RandomAccessFile(int fd, uint64_t size, std::filesystem::path path);

  int fd_ = -1;
  uint64_t size_ = 0;
  std::filesystem::path path_;
};

// Buffered append-only writer.
class WritableFile {
 public:
  // Truncates any existing file.
  static Result<std::unique_ptr<WritableFile>> Create(const std::filesystem::path& path);
  // Opens for append, creating it if absent. Used by the WAL, which must
  // continue an existing log rather than replace it.
  static Result<std::unique_ptr<WritableFile>> OpenForAppend(const std::filesystem::path& path);
  ~WritableFile();

  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  Status Append(std::string_view data);
  Status Flush();
  // Flush plus a real durability barrier. On macOS that means F_FULLFSYNC:
  // plain fsync() does not force the drive's own write cache, so using it
  // would make a durability claim this code cannot honour.
  Status Sync();
  Status Close();

  uint64_t Offset() const { return offset_; }

 private:
  WritableFile(int fd, std::filesystem::path path);

  int fd_ = -1;
  std::filesystem::path path_;
  std::string buffer_;
  uint64_t offset_ = 0;
};

// fsync the directory itself, which is what makes a newly created file's
// *name* durable. Without it, a crash can leave a fully-synced file that no
// longer appears in its directory -- the classic rename-durability trap.
Status SyncDirectory(const std::filesystem::path& dir);

}  // namespace raftkv::lsm
