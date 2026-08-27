#pragma once

// The replicated log, in memory.
//
// Phase 2 keeps entries in a vector; Phase 3 puts a durable WAL behind the
// same interface. `offset_` is the index of the first entry held, which is one
// past the last index covered by a snapshot -- so compaction later is a matter
// of dropping a prefix and moving the offset, not of reworking callers.
//
// Index 0 never holds an entry. The log is 1-indexed so that "index 0" can
// mean "before the beginning" without a sentinel, which matches the paper and
// avoids off-by-one translation at every call site.

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "lsm/status.h"
#include "raft.pb.h"

namespace raftkv::raft {

using lsm::Result;
using lsm::Status;

class RaftLog {
 public:
  RaftLog() = default;

  // Index of the last entry, or the snapshot's last included index if empty.
  uint64_t LastIndex() const;

  // Term of the entry at `index`.
  //
  // Returns 0 for index 0 (the well-defined "before the beginning"), and
  // kNotFound if `index` has been compacted away or is past the end. The
  // distinction matters: treating a compacted index as term 0 would make a
  // stale AppendEntries appear to match.
  Result<uint64_t> TermAt(uint64_t index) const;

  uint64_t FirstIndex() const { return offset_ + 1; }

  bool Empty() const { return entries_.empty(); }
  size_t Size() const { return entries_.size(); }

  const proto::Entry* At(uint64_t index) const;

  // Entries in [lo, hi], clamped to what the log holds.
  std::vector<proto::Entry> Slice(uint64_t lo, uint64_t hi) const;

  // Appends entries whose indices must continue from LastIndex().
  Status Append(std::span<const proto::Entry> entries);

  // Applies a leader's entries starting after `prev_index`.
  //
  // Existing entries that conflict (same index, different term) are truncated
  // along with everything after them; entries that already match are left
  // alone. Truncating on a match instead would discard entries the leader is
  // about to resend, and worse, could discard a committed entry that arrived
  // via a different path.
  //
  // `first_changed` receives the lowest index whose CONTENT changed, or 0 if
  // nothing did. The caller needs this to know what must be re-persisted:
  // comparing lengths before and after is not enough, because a truncation
  // followed by a refill of the same length leaves LastIndex() unchanged while
  // rewriting entries underneath it.
  Status TruncateAndAppend(uint64_t prev_index, std::span<const proto::Entry> entries,
                           uint64_t* first_changed = nullptr);

  // Is `candidate_last_{term,index}` at least as up to date as this log?
  // Compares term first, then index -- the order matters, and reversing it
  // permits electing a leader that is missing committed entries.
  bool IsUpToDate(uint64_t candidate_last_term, uint64_t candidate_last_index) const;

  // Drops entries at or below `index`, which a snapshot now covers.
  void CompactTo(uint64_t index, uint64_t index_term);

  uint64_t SnapshotIndex() const { return offset_; }
  uint64_t SnapshotTerm() const { return offset_term_; }

 private:
  std::vector<proto::Entry> entries_;
  // Index of the last entry covered by a snapshot; entries_ starts at
  // offset_ + 1.
  uint64_t offset_ = 0;
  uint64_t offset_term_ = 0;
};

}  // namespace raftkv::raft
