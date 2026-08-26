#pragma once

// Write-ahead log: append-only, CRC-per-record, torn-write detection.
//
// This is the LSM's own WAL and is deliberately separate from the Raft log
// (src/storage/, Phase 3). They hold different things: the Raft log holds
// proposed commands awaiting consensus, this holds the effects of commands
// already applied.
//
// Durability policy, and the reason it is not what you might expect:
//
// Apply does NOT fsync. What Apply requires is atomicity of
// (effect, last_applied_index), not durability. If the process dies with
// buffered records unwritten, last_applied_index rolls back with the effects
// it was batched alongside -- they are in the same record, so CRC truncation
// drops them together -- and Raft replays from that index out of its own
// durable log. Nothing is lost that cannot be reconstructed.
//
// The invariant that must hold instead: never compact the Raft log past what
// this WAL has durably persisted. Sync() is therefore called before a
// snapshot allows Raft log truncation, not on every write.
//
// Record format:
//   crc32c  : fixed32  (over length + payload)
//   length  : fixed32
//   payload : length bytes

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "lsm/status.h"

namespace raftkv::lsm {

class WalWriter {
 public:
  static Result<std::unique_ptr<WalWriter>> Open(const std::filesystem::path& path);
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // Appends one record. Buffered; not durable until Sync().
  Status Append(std::string_view payload);

  // Flushes userspace buffers to the kernel, then fsyncs.
  Status Sync();

  // Bytes appended since Open, including record headers.
  uint64_t BytesWritten() const { return bytes_written_; }

  Status Close();

 private:
  WalWriter(int fd, std::filesystem::path path);

  Status FlushBuffer();

  int fd_ = -1;
  std::filesystem::path path_;
  std::string buffer_;
  uint64_t bytes_written_ = 0;
};

class WalReader {
 public:
  static Result<std::unique_ptr<WalReader>> Open(const std::filesystem::path& path);

  // Reads the next record into `record`.
  //
  // Returns kNotFound at clean end of file. On a short or CRC-failing record
  // -- which is what a crash mid-append leaves behind -- reports the byte
  // offset where the damage starts via TruncateOffset() and returns
  // kCorruption. The caller decides whether that is a recoverable tail (it is,
  // if it is the last record) or real corruption in the middle of the file.
  Status ReadRecord(std::string* record);

  // Offset of the first byte that failed to parse.
  uint64_t TruncateOffset() const { return good_offset_; }

  bool AtEnd() const { return offset_ >= contents_.size(); }

 private:
  explicit WalReader(std::string contents);

  std::string contents_;
  size_t offset_ = 0;
  uint64_t good_offset_ = 0;
};

// Reads every intact record from `path`, stopping at the first damaged one.
// If `truncate_partial` is set, the file is truncated to the last good record
// so that subsequent appends start from a consistent point.
Status ReplayWal(const std::filesystem::path& path, bool truncate_partial,
                 std::vector<std::string>* records);

}  // namespace raftkv::lsm
