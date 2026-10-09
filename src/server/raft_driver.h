#pragma once

// Puts a RaftNode on a real network and a real disk, and drives it.
//
// This is the concrete implementation of the Ready/Advance contract that
// test/raft_harness.h imitates in memory: persist hard state and entries,
// THEN send messages, then apply committed entries, then Advance. One thread
// owns the RaftNode's state (protected by mutex_); everything else --
// inbound RPC handlers, the ticker, outbound response delivery -- funnels
// through that lock rather than touching the core directly. That is what
// "one thread drives the Ready loop per raft group" means in practice: not
// literally one OS thread, but one logical owner enforced by a mutex.
//
// Persistence split, and why the split is not arbitrary:
//   - RequestVote/AppendEntries handlers persist BEFORE returning, because
//     the RPC's return value is itself the reply -- there is no second chance
//     to withhold it.
//   - Tick()-driven leader activity persists before dispatching outbound
//     RPCs, for the same reason applied one level up: Send() must never race
//     ahead of Sync().
//
// Outbound dispatch is asynchronous. A leader's AppendEntries to N followers
// must not block on the slowest one, or one stuck peer stalls replication to
// every other. Responses re-enter through Step(), under the same lock, from
// whatever thread the gRPC client callback lands on.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "lsm/status.h"
#include "raft.grpc.pb.h"

#include "raft/raft_node.h"
#include "statemachine/state_machine.h"
#include "storage/raft_storage.h"

namespace raftkv::server {

using lsm::Result;
using lsm::Status;

struct DriverOptions {
  raft::Config raft_config;
  std::filesystem::path storage_dir;
  // node id -> "host:port". Must include every peer in raft_config.peers.
  std::unordered_map<uint64_t, std::string> peer_addresses;

  uint32_t tick_interval_ms = 20;
};

// Owns one Raft group end to end: the deterministic core, its durable log and
// hard state, its state machine, and its gRPC peers.
class RaftDriver {
 public:
  static Result<std::unique_ptr<RaftDriver>> Open(
      DriverOptions options, std::shared_ptr<statemachine::StateMachine> state_machine);

  ~RaftDriver();

  RaftDriver(const RaftDriver&) = delete;
  RaftDriver& operator=(const RaftDriver&) = delete;

  // Starts the ticker thread. Idempotent.
  void Start();
  // Stops the ticker and waits for in-flight outbound dispatch to settle.
  // Idempotent, and required before destruction.
  void Stop();

  // Proposes a command. Fails with kNotSupported if this node is not the
  // leader; the caller should redirect using LeaderHint().
  Status Propose(std::span<const std::byte> command);

  // Proposes `command` and waits until it has been applied, returning the state
  // machine's result for it.
  //
  //   kNotSupported -- not the leader; nothing was proposed. Redirect using
  //                    LeaderHint().
  //   kUnavailable  -- the entry was overwritten by a new leader's log, so it
  //                    definitely did NOT commit.
  //   kTimedOut     -- gave up waiting. The command MAY still commit later, so
  //                    the outcome is unknown. A retry is safe only because
  //                    commands carry (client_id, seq) and the state machine
  //                    deduplicates.
  Result<statemachine::ApplyResult> ProposeAndWait(std::span<const std::byte> command,
                                                   std::chrono::milliseconds timeout);

  // Linearizable read: obtain a read point from Raft (ReadIndex), wait until
  // the state machine has applied through it, then read. Same error meanings
  // as ProposeAndWait; a read has no "outcome unknown" problem, so kTimedOut
  // here just means "try again".
  Result<std::optional<std::string>> LinearizableGet(std::string_view key,
                                                     std::chrono::milliseconds timeout);

  // Address of a peer (or self), for building a redirect. Empty if unknown.
  std::string AddressOf(uint64_t node_id) const;

  // Highest Raft index the driver has fully processed, including entries that
  // carry no command. This, not the state machine's own applied index, is what
  // a read must wait on: the state machine skips no-ops, so its index can trail
  // Raft's (DECISIONS.md D-0037).
  uint64_t AppliedRaftIndex() const;

  // Number of committed entries handed to the apply step more than once, or out
  // of order. The core must deliver each committed entry exactly once, in
  // order; anything above zero means it is re-emitting history, which the state
  // machine's index-based dedup would silently absorb while every commit costs
  // O(log length). Exposed so tests can assert it stays zero.
  uint64_t RedeliveredEntries() const;

  // Inbound RPC handlers. Each is synchronous: the returned Message IS the
  // reply, already extracted from a Ready that has been fully persisted.
  Status HandleRequestVote(const proto::Message& request, proto::Message* response);
  Status HandleAppendEntries(const proto::Message& request, proto::Message* response);

  uint64_t Id() const { return options_.raft_config.id; }
  raft::Role CurrentRole() const;
  uint64_t Term() const;
  uint64_t CommitIndex() const;
  // Best current guess at the leader, for a client to redirect to. 0 if
  // unknown.
  uint64_t LeaderHint() const;

  // Redials `peer` at a new address. Used when a peer restarts on a fresh
  // ephemeral port; a production deployment with fixed ports never needs
  // this, but nothing stops it from being useful there too if an operator
  // moves a node.
  void UpdatePeerAddress(uint64_t peer, const std::string& address);

 private:
  RaftDriver(DriverOptions options, std::shared_ptr<statemachine::StateMachine> state_machine,
             std::unique_ptr<storage::HardStateStore> hard_state,
             std::unique_ptr<storage::RaftLogStore> log);

  Status Recover();

  // Drains Ready under mutex_ (already held by the caller), persists it, and
  // returns the entries that must now be applied and the messages that must
  // now be dispatched -- both handed back to the caller so persistence
  // happens INSIDE the lock but the (slower) apply and dispatch work can
  // happen outside it, without another thread observing a half-drained Ready.
  struct DrainedReady {
    std::vector<proto::Message> outbound;
    std::vector<raft::ReadState> read_states;
    std::vector<std::string> dropped_reads;
  };
  Status DrainAndPersist(DrainedReady* out);

  void TickLoop();
  void DispatchOutbound(std::vector<proto::Message> outbound);
  void SendOne(const proto::Message& message);
  // Everything that happens after a Ready has been persisted and the lock
  // released: apply committed entries in order, resolve finished reads, send
  // messages. Every call site goes through here so none can forget a step.
  void Finish(DrainedReady drained);
  void ApplyPending();
  void ApplyBatch(const std::vector<proto::Entry>& entries);
  void ResolveReads(const std::vector<raft::ReadState>& states,
                    const std::vector<std::string>& dropped);

  proto::Raft::Stub* StubFor(uint64_t peer);

  DriverOptions options_;
  std::shared_ptr<statemachine::StateMachine> state_machine_;

  mutable std::mutex mutex_;
  std::unique_ptr<raft::RaftNode> node_;
  std::unique_ptr<storage::HardStateStore> hard_state_;
  std::unique_ptr<storage::RaftLogStore> log_;

  // Guards stubs_ against concurrent redial (UpdatePeerAddress) and lookup
  // (StubFor, called from detached dispatch threads).
  mutable std::mutex stubs_mutex_;
  std::unordered_map<uint64_t, std::unique_ptr<proto::Raft::Stub>> stubs_;

  // Committed entries waiting to be applied, in commit order. Filled under
  // mutex_ by DrainAndPersist; drained by ApplyPending under apply_mutex_.
  //
  // Why a queue and not "apply what you just drained": two threads can drain
  // consecutive batches, say [5] and [6], then race to apply them after
  // releasing mutex_. If 6 lands first the state machine records 6 as applied
  // and then treats 5 as an already-applied duplicate -- silently never
  // applying it. One queue plus one apply mutex makes the apply order equal
  // the commit order by construction.
  std::deque<proto::Entry> apply_queue_;
  std::mutex apply_mutex_;

  // Waiters. One mutex and one condition variable for all of them: contention
  // here is negligible next to an fsync, and a single lock cannot be taken in
  // two orders.
  struct WriteWaiter {
    uint64_t term = 0;
    bool done = false;
    Status status;
    statemachine::ApplyResult result;
  };
  struct ReadWaiter {
    bool done = false;
    bool dropped = false;
    uint64_t index = 0;
  };
  mutable std::mutex waiters_mutex_;
  std::condition_variable waiters_cv_;
  std::map<uint64_t, WriteWaiter> write_waiters_;  // keyed by log index
  std::map<std::string, ReadWaiter> read_waiters_;
  uint64_t applied_raft_index_ = 0;  // guarded by waiters_mutex_
  std::atomic<uint64_t> read_counter_{0};
  uint64_t last_batch_index_ = 0;  // guarded by apply_mutex_
  std::atomic<uint64_t> redelivered_{0};
  const std::string read_epoch_;

  std::thread ticker_;
  std::atomic<bool> running_{false};

  // Outbound RPCs are fire-and-forget from the caller's perspective but must
  // not outlive the driver, so in-flight dispatch is tracked and drained by
  // Stop().
  std::mutex inflight_mutex_;
  uint64_t inflight_count_ = 0;
  std::condition_variable inflight_drained_;
};

}  // namespace raftkv::server
