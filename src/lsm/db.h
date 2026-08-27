#pragma once

// The LSM engine.
//
// Read path, newest source first:
//   1. the active memtable
//   2. the immutable memtable, if a flush is in progress
//   3. L0 SSTables, newest file first
//   4. the L1 SSTable, if compaction has produced one
// The first source that yields either a value OR a tombstone wins. Treating a
// tombstone as "keep looking" resurrects deleted keys from older levels, which
// is the single easiest way to break a delete in an LSM.
//
// Durability, and why Write() does not fsync by default:
//
// The Raft state machine needs (effect, last_applied_index) to be ATOMIC, not
// durable. Both live in the same WriteBatch, so they reach the WAL in one
// record; a crash mid-append leaves a bad CRC and recovery discards the whole
// record, rolling back the effect and the index together. Whatever is lost is
// replayable from Raft's own durable log starting at the recovered index.
//
// The invariant this trades for the fsync is: never compact the Raft log past
// what this engine has durably persisted. SyncWal() exists for exactly that
// moment and is called before snapshot-driven Raft log truncation, not on
// every write. WriteOptions::sync forces the naive behaviour, which Phase 8
// needs as its baseline configuration.
//
// Concurrency: a single writer, many readers. Write() serializes on a mutex;
// Get() takes a shared lock only long enough to copy the set of table handles
// it will read, then reads outside the lock. SSTables are immutable and held
// by shared_ptr, so a compaction that retires a file cannot pull it out from
// under a reader that is mid-scan.

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "lsm/memtable.h"
#include "lsm/sstable.h"
#include "lsm/status.h"
#include "lsm/wal.h"
#include "lsm/write_batch.h"

namespace raftkv::lsm {

struct Options {
  std::filesystem::path dir;

  // Flush the active memtable once it exceeds this many bytes.
  size_t memtable_bytes = size_t{4} * 1024 * 1024;

  // Merge L0 into L1 once this many L0 files exist.
  size_t l0_compaction_trigger = 4;

  // Make every Write() durable before returning. This is the Phase 8 baseline
  // configuration (--fsync_per_entry=true); see the durability note above for
  // why it is not the default.
  bool sync_on_write = false;
};

struct WriteOptions {
  // Overrides Options::sync_on_write for a single call.
  std::optional<bool> sync;
};

// A consistent point-in-time view. Holds a sequence number and pins the table
// files live at the moment it was taken, so compaction cannot delete them
// while it exists.
class Snapshot {
 public:
  SequenceNumber Sequence() const { return sequence_; }

 private:
  friend class DB;
  Snapshot(SequenceNumber sequence, std::vector<std::shared_ptr<SsTable>> tables)
      : sequence_(sequence), pinned_(std::move(tables)) {}

  SequenceNumber sequence_;
  std::vector<std::shared_ptr<SsTable>> pinned_;
};

class DB {
 public:
  static Result<std::unique_ptr<DB>> Open(Options options);
  ~DB();

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  // Applies every mutation in `batch` atomically.
  Status Write(WriteBatch& batch, const WriteOptions& options = {});

  Status Put(std::string_view key, std::string_view value, const WriteOptions& options = {});
  Status Delete(std::string_view key, const WriteOptions& options = {});

  // Returns nullopt when the key is absent or deleted.
  Result<std::optional<std::string>> Get(std::string_view key,
                                         const Snapshot* snapshot = nullptr) const;

  std::shared_ptr<Snapshot> TakeSnapshot();

  // Makes everything written so far durable. Call before allowing the Raft log
  // to be truncated past LastSequence().
  Status SyncWal();

  // Highest sequence number assigned so far.
  SequenceNumber LastSequence() const;

  // Forces the active memtable to disk. Exposed for tests and for snapshot
  // creation; normal operation flushes on the size trigger.
  Status FlushMemTable();

  // Merges every L0 table and L1 into a single new L1 table.
  Status CompactRange();

  // Visits every live key exactly once, in ascending user-key order, skipping
  // deleted keys.
  //
  // Implemented by flushing and compacting down to a single table and scanning
  // it, rather than by merging iterators across the memtable and every level.
  // That is simple and obviously correct, and it costs a full compaction --
  // this call blocks writes for its duration. A merging iterator is the
  // documented replacement if snapshot latency shows up in a profile.
  Status ScanAll(const std::function<Status(std::string_view key, std::string_view value)>& fn);

  // Deletes all data and resets to an empty database. Used by snapshot
  // restore, which must replace state wholesale rather than merge into it:
  // merging would leave keys that the snapshot's author had deleted.
  Status DestroyContents();

  // Introspection for tests.
  size_t NumL0Tables() const;
  bool HasL1Table() const;

 private:
  explicit DB(Options options);

  Status Recover();
  Status MaybeFlush();
  Status WriteLevel0Table(const MemTable& table, const std::filesystem::path& path);
  Status LoadTables();

  std::filesystem::path TablePath(uint64_t number) const;
  std::filesystem::path WalPath() const;

  Options options_;

  // Serializes writers. One writer at a time; readers do not take it.
  mutable std::mutex write_mutex_;

  // Guards the mutable view of the world: memtables, table lists, sequence.
  mutable std::shared_mutex state_mutex_;

  std::unique_ptr<MemTable> mem_;
  std::unique_ptr<WalWriter> wal_;
  std::vector<std::shared_ptr<SsTable>> l0_;  // newest first
  std::shared_ptr<SsTable> l1_;
  SequenceNumber last_sequence_ = 0;
  uint64_t next_file_number_ = 1;
};

}  // namespace raftkv::lsm
