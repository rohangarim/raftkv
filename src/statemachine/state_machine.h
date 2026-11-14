#pragma once

// The interface Raft sees. Deliberately narrow: the consensus core must never
// learn anything about storage, and this header is the whole contract between
// them.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "kv.pb.h"
#include "minilsm/status.h"

namespace raftkv::statemachine {

using lsm::Result;
using lsm::Status;

// Outcome of applying one command.
// Whether a key (or range of keys) belongs to this group.
enum class RangeVerdict { kOk, kWrongShard, kMoving };

// What a transaction command found, beyond plain success.
enum class TxnOutcome {
  kNone,
  kLocked,            // another transaction holds a key
  kValidationFailed,  // a validated read no longer holds
  kAborted,           // the transaction was rolled back (or never could commit)
  kAlreadyCommitted,  // asked to roll back something that already committed
};

struct ApplyResult {
  // Non-Ok means the command was malformed or could not be applied. It does
  // NOT mean "the operation reported a negative answer" -- a CAS that fails
  // its comparison is a successful apply with cas_mismatch set.
  Status status;

  // Present for reads and for the observed value of a failed CAS.
  std::optional<std::string> value;

  // The comparison failed; nothing was written.
  bool cas_mismatch = false;

  // The command touched keys this group does not own (nothing was written), or
  // keys inside a range that is being handed over.
  bool wrong_shard = false;
  bool moving = false;

  TxnOutcome txn = TxnOutcome::kNone;
  // The lock that blocked the command, when txn == kLocked.
  std::optional<proto::LockRecord> lock;

  // For a batch whose precondition failed: which check, by position.
  std::optional<uint32_t> failed_check;

  // The command was a repeat of one already applied, and this result came from
  // the session cache rather than from applying it a second time.
  bool deduplicated = false;
};

// An opaque, serialized point-in-time image of the state machine.
//
// Contains the user data, the applied index, AND the client session table.
// Omitting the session table would break exactly-once semantics after a
// restore: a client's retry would look new and be applied twice.
class Snapshot {
 public:
  virtual ~Snapshot() = default;

  virtual uint64_t LastIncludedIndex() const = 0;
  virtual uint64_t LastIncludedTerm() const = 0;

  // Total serialized size, so the transport can decide how to chunk it.
  virtual uint64_t Size() const = 0;

  // Reads up to `max_bytes` starting at `offset`. Phase 5 streams a snapshot
  // in chunks rather than materializing it in one gRPC message.
  virtual Result<std::string> ReadChunk(uint64_t offset, size_t max_bytes) const = 0;
};

// A snapshot held entirely in memory, for restoring from bytes that already
// came off disk or the wire.
class BytesSnapshot final : public Snapshot {
 public:
  BytesSnapshot(uint64_t index, uint64_t term, std::string bytes)
      : index_(index), term_(term), bytes_(std::move(bytes)) {}

  uint64_t LastIncludedIndex() const override { return index_; }
  uint64_t LastIncludedTerm() const override { return term_; }
  uint64_t Size() const override { return bytes_.size(); }
  Result<std::string> ReadChunk(uint64_t offset, size_t max_bytes) const override {
    if (offset > bytes_.size()) {
      return Status::InvalidArgument("snapshot chunk offset past end");
    }
    return bytes_.substr(offset, std::min<size_t>(max_bytes, bytes_.size() - offset));
  }

 private:
  uint64_t index_;
  uint64_t term_;
  std::string bytes_;
};

class StateMachine {
 public:
  virtual ~StateMachine() = default;

  // Applies the command at log position `index`.
  //
  // Must be deterministic: the same command sequence must produce identical
  // state on every replica. Must also be idempotent with respect to `index` --
  // replaying an index at or below LastAppliedIndex() must not apply it again,
  // because recovery replays the tail of the Raft log.
  virtual ApplyResult Apply(uint64_t index, std::span<const std::byte> command) = 0;

  virtual Result<std::unique_ptr<Snapshot>> TakeSnapshot(uint64_t index, uint64_t term) = 0;

  // Replaces all state with the snapshot's contents.
  virtual Status RestoreSnapshot(Snapshot& snapshot) = 0;

  virtual uint64_t LastAppliedIndex() const = 0;

  // Reads the current value of `key`; nullopt if absent. This is a local read
  // with NO freshness guarantee of its own -- a caller that needs a
  // linearizable read must first obtain a read point from Raft (ReadIndex) and
  // wait until LastAppliedIndex covers it.
  virtual Result<std::optional<std::string>> Get(std::string_view key) const = 0;

  // Visits user keys in [start, end) in ascending order, at most `limit` of
  // them (0 = unlimited); an empty `end` is unbounded. Like Get, a local read
  // with no freshness guarantee of its own.
  virtual Status Scan(
      std::string_view start, std::string_view end, size_t limit,
      const std::function<Status(std::string_view, std::string_view)>& fn) const = 0;

  // Judges [start, end) against the owned range; an empty `end` means
  // unbounded. With `write` set, a fenced range counts as kMoving. A point key
  // is start == end.
  virtual RangeVerdict CheckRange(std::string_view start, std::string_view end,
                                  bool write) const = 0;

  // The first lock held by an in-flight transaction that a read of [start, end)
  // must not step over: one that will change a key on commit. Locks that only
  // hold a key against writers are invisible to readers. A point key is
  // start == end.
  virtual std::optional<proto::LockRecord> FindBlockingLock(std::string_view start,
                                                            std::string_view end) const = 0;

  // Span forms of the two checks above, for scans. In CheckRange and
  // FindBlockingLock `start == end` means one key, which makes a scan of the
  // whole key space (start "" and end "" for "unbounded") indistinguishable from
  // a lookup of the key "": such a scan was judged as a single key, so a shard
  // that had given away half its range still answered an unbounded scan with
  // only its own half and no error. These always treat the arguments as a range.
  // The defaults forward for state machines that do not tell the cases apart.
  virtual RangeVerdict CheckSpan(std::string_view start, std::string_view end) const {
    return CheckRange(start, end, false);
  }
  virtual std::optional<proto::LockRecord> FindBlockingLockInSpan(std::string_view start,
                                                                  std::string_view end) const {
    return FindBlockingLock(start, end);
  }

  // "C" committed, "A" aborted, or nullopt: no decision is recorded here.
  virtual Result<std::optional<std::string>> TxnDecision(std::string_view txid) const = 0;

  // Makes everything applied so far durable.
  //
  // The Raft layer MUST call this before compacting its log past
  // LastAppliedIndex(). Apply() does not fsync -- see DECISIONS.md D-0014 --
  // so the durability of applied state is this call's responsibility, and
  // truncating the Raft log without it loses data that nothing else can
  // reconstruct.
  virtual Status Sync() = 0;
};

}  // namespace raftkv::statemachine
