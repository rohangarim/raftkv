#pragma once

// The interface Raft sees. Deliberately narrow: the consensus core must never
// learn anything about storage, and this header is the whole contract between
// them.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "lsm/status.h"

namespace raftkv::statemachine {

using lsm::Result;
using lsm::Status;

// Outcome of applying one command.
struct ApplyResult {
  // Non-Ok means the command was malformed or could not be applied. It does
  // NOT mean "the operation reported a negative answer" -- a CAS that fails
  // its comparison is a successful apply with cas_mismatch set.
  Status status;

  // Present for reads and for the observed value of a failed CAS.
  std::optional<std::string> value;

  // The comparison failed; nothing was written.
  bool cas_mismatch = false;

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
