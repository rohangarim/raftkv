#pragma once

// Durable Raft state: the replicated log, and the hard state Figure 2 requires
// to survive a crash.
//
// This is NOT the LSM WAL. That one records the effects of commands already
// applied and may lag durability, because anything it loses can be replayed
// from here (see DECISIONS.md D-0014). This log has nothing behind it: an
// entry acknowledged from here and then lost is a lost committed write. So the
// durability rules are the opposite way round, and the two must never share a
// file.
//
// Avoiding an fsync per entry, without ever acknowledging an entry we might
// lose:
//
//   Entries are appended to a buffer and made durable ONE READY BATCH AT A
//   TIME. The Ready/Advance contract already forbids sending any message
//   before the batch it accompanies is persisted, so a single fsync covers
//   every entry that accumulated during that round, and no AppendEntries
//   response leaves the node until it returns. Under load the batch grows and
//   the fsync amortises across it; when idle it degenerates to one fsync for
//   one entry, which is both correct and cheap because nothing else is
//   happening.
//
//   The rule, stated so it can be checked: Sync() must return before any
//   message from that Ready is sent, and before the leader counts its own
//   entries toward match_index.
//
// Segment layout. The log is a directory of files named `<first_index>.log`,
// each holding records:
//
//   crc32c : fixed32   (over length + payload)
//   length : fixed32
//   payload: serialized proto::Entry
//
// Segmenting exists so compaction after a snapshot is a file unlink rather
// than a rewrite of the whole log.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "lsm/file.h"
#include "lsm/status.h"
#include "raft.pb.h"

namespace raftkv::storage {

using lsm::Result;
using lsm::Status;

// currentTerm and votedFor, written durably before any RPC is answered.
//
// Written whole to a temp file and renamed, rather than updated in place. A
// torn in-place update of an 8-byte term field is unlikely but not impossible,
// and the recovery story for "the term is half of two numbers" is that there
// is no recovery story. Rename is atomic; the directory fsync makes the rename
// itself durable.
class HardStateStore {
 public:
  static Result<std::unique_ptr<HardStateStore>> Open(const std::filesystem::path& dir);

  // Durable when this returns.
  Status Save(const proto::HardState& state);

  const proto::HardState& Current() const { return current_; }

 private:
  HardStateStore(std::filesystem::path dir, proto::HardState current);

  std::filesystem::path dir_;
  proto::HardState current_;
};

struct LogStoreOptions {
  std::filesystem::path dir;
  // Roll to a new segment past this size. Small enough that compaction can
  // actually free space, large enough that rolling is rare.
  uint64_t segment_bytes = uint64_t{16} * 1024 * 1024;
};

class RaftLogStore {
 public:
  static Result<std::unique_ptr<RaftLogStore>> Open(LogStoreOptions options);

  // Buffers entries. NOT durable until Sync().
  Status Append(std::span<const proto::Entry> entries);

  // Makes every buffered entry durable. Must return before any message from
  // the same Ready batch is sent.
  Status Sync();

  // Drops entries at and after `index`, for a follower repairing a divergent
  // suffix. Durable when this returns: a truncation that is not durable can
  // resurrect the discarded entries on restart, which is exactly the bug the
  // Phase 2 harness caught in the in-memory path (D-0029).
  Status TruncateSuffix(uint64_t index);

  // Drops entries at and below `index`, now covered by a snapshot. Only whole
  // segments are removed, so this is a lower bound, not exact.
  Status CompactPrefix(uint64_t index);

  // Every entry on disk, in index order. Used once at startup.
  Result<std::vector<proto::Entry>> ReadAll();

  uint64_t FirstIndex() const { return first_index_; }
  uint64_t LastIndex() const { return last_index_; }
  uint64_t SegmentCount() const { return segments_.size(); }

 private:
  struct Segment {
    uint64_t first_index = 0;
    std::filesystem::path path;
  };

  RaftLogStore(LogStoreOptions options, std::vector<Segment> segments);

  Status OpenActiveSegment();
  Status RollSegment(uint64_t next_index);

  LogStoreOptions options_;
  std::vector<Segment> segments_;
  std::unique_ptr<lsm::WritableFile> active_;
  uint64_t first_index_ = 1;
  uint64_t last_index_ = 0;
  // Entries appended since the last Sync(). Nothing here may be acknowledged.
  uint64_t unsynced_entries_ = 0;
};

}  // namespace raftkv::storage
