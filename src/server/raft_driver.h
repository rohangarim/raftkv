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
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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
    std::vector<proto::Entry> committed_entries;
    std::vector<proto::Message> outbound;
  };
  Status DrainAndPersist(DrainedReady* out);

  void TickLoop();
  void DispatchOutbound(std::vector<proto::Message> outbound);
  void SendOne(const proto::Message& message);
  void ApplyCommitted(const std::vector<proto::Entry>& entries);

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
