#pragma once

// The consensus core: no threads, no sockets, no wall clock.
//
// Everything time-based goes through Tick(), and every input is a Message.
// That makes the whole node a pure function of (persisted state, messages,
// ticks), which is what lets a five-node cluster run inside one unit test
// against a deliberately hostile simulated network -- with a seed, so a
// failure is a reproducible bug report rather than an anecdote.
//
// The Ready/Advance contract, which exists to make Figure 2's durability rule
// enforceable rather than merely documented:
//
//   1. caller obtains a Ready
//   2. caller PERSISTS ready.hard_state and ready.entries_to_persist, durably
//   3. only then does the caller send ready.messages
//   4. caller applies ready.committed_entries
//   5. caller calls Advance(ready)
//
// Step 2 before step 3 is the requirement that currentTerm and votedFor are
// durable before responding to any RPC. Reversing them lets a node vote twice
// in one term across a crash, which breaks Election Safety.

#include <cstdint>
#include <functional>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "raft.pb.h"

#include "raft/raft_log.h"

namespace raftkv::raft {

enum class Role : uint8_t {
  kFollower,
  kCandidate,
  kLeader,
};

const char* RoleName(Role role);

struct Config {
  uint64_t id = 0;
  std::vector<uint64_t> peers;  // includes `id`

  // Election timeout is drawn uniformly from
  // [election_tick, 2 * election_tick) on every reset. Randomization is what
  // stops candidates from repeatedly splitting the vote in lockstep.
  uint32_t election_tick = 10;
  uint32_t heartbeat_tick = 1;

  // Seeded so a test run is reproducible.
  uint64_t random_seed = 0;

  // Cap on entries in a single AppendEntries.
  size_t max_entries_per_append = 64;
};

// A linearizable read point: once the state machine has applied through
// `index`, a read is safe to serve. `context` is the caller's own token from
// ReadIndex(), echoed back so the caller can match it to the waiting request.
struct ReadState {
  std::string context;
  uint64_t index = 0;
};

struct InstalledSnapshot {
  uint64_t index = 0;
  uint64_t term = 0;
  std::string data;
};

// Work the caller must carry out before the core may proceed.
struct Ready {
  // Set only when it changed since the last Advance. Must be made durable
  // before any message in `messages` is sent.
  std::optional<proto::HardState> hard_state;

  // New log entries to persist, also before sending.
  std::vector<proto::Entry> entries_to_persist;

  // Outgoing messages. Send only after the two fields above are durable.
  std::vector<proto::Message> messages;

  // Committed entries the state machine should apply, in order.
  std::vector<proto::Entry> committed_entries;

  // ReadIndex requests whose leadership has now been confirmed by a quorum.
  std::vector<ReadState> read_states;

  // ReadIndex requests that can never complete because this node stopped being
  // leader first. The caller should fail them now rather than wait for a
  // timeout.
  std::vector<std::string> dropped_reads;

  // A snapshot received from the leader. The caller must persist it (and drop
  // the log it replaces) and restore its state machine from it BEFORE sending
  // `messages` and before applying `committed_entries`, since the reply
  // acknowledges that the snapshot is durable.
  std::optional<InstalledSnapshot> snapshot;

  bool Empty() const {
    return !hard_state.has_value() && entries_to_persist.empty() && messages.empty() &&
           committed_entries.empty() && read_states.empty() && dropped_reads.empty() &&
           !snapshot.has_value();
  }
};

class RaftNode {
 public:
  explicit RaftNode(Config config);

  // Restores durable state after a restart. Everything else -- role, commit
  // index, leader -- is volatile and rebuilt by the protocol.
  void RestoreHardState(const proto::HardState& hard_state);
  Status RestoreLog(std::span<const proto::Entry> entries);
  // Call after RestoreHardState and BEFORE RestoreLog: the log that follows
  // starts at index + 1.
  void RestoreSnapshot(uint64_t index, uint64_t term, std::string data);

  // The application has captured its state through `index` (which must be
  // applied) as `snapshot_data`. Drops the log prefix and keeps the data to
  // send to followers that fall behind it.
  Status CompactLog(uint64_t index, std::string snapshot_data);

  void Step(const proto::Message& message);
  void Tick();

  // Appends to the leader's log. Fails if this node is not the leader; the
  // caller is expected to redirect using LeaderId().
  // On success, `index` and `term` (if given) say where the entry landed. A
  // caller that wants to know whether ITS command committed must compare both:
  // the same index can later hold a different term's entry if leadership
  // changes before this one commits.
  Status Propose(std::span<const std::byte> command, uint64_t* index = nullptr,
                 uint64_t* term = nullptr);

  // Starts a linearizable read. The leader records its commit index, confirms
  // it is still leader by getting heartbeat acks from a quorum for THIS
  // request, and then reports (context, index) in a later Ready. The caller
  // serves the read once its state machine has applied through `index`.
  //
  // Errors, and why they are different:
  //   kNotSupported -- not the leader; redirect using LeaderId().
  //   kUnavailable  -- leader, but it has not yet committed an entry from its
  //                    own term, so its commit index may be stale (the Figure 8
  //                    hazard, applied to reads). Retry shortly.
  //   kInvalidArgument -- `context` is already pending.
  Status ReadIndex(std::string context);

  Ready ReadyToProcess();
  void Advance(const Ready& ready);

  uint64_t Id() const { return config_.id; }
  Role CurrentRole() const { return role_; }
  uint64_t Term() const { return term_; }
  uint64_t Vote() const { return vote_; }
  uint64_t CommitIndex() const { return commit_index_; }
  uint64_t LeaderId() const { return leader_id_; }
  const RaftLog& Log() const { return log_; }

  // Test hooks.
  uint64_t MatchIndex(uint64_t peer) const;
  uint64_t NextIndex(uint64_t peer) const;

 private:
  struct PeerProgress {
    uint64_t next_index = 1;
    uint64_t match_index = 0;
  };

  void BecomeFollower(uint64_t term, uint64_t leader);
  void BecomeCandidate();
  void BecomeLeader();

  void ResetElectionTimer();
  size_t Quorum() const { return config_.peers.size() / 2 + 1; }

  void HandleRequestVote(const proto::Message& m);
  void HandleRequestVoteResponse(const proto::Message& m);
  void HandleAppend(const proto::Message& m);
  void HandleAppendResponse(const proto::Message& m);
  void HandleSnapshot(const proto::Message& m);
  void HandleSnapshotResponse(const proto::Message& m);
  void SendSnapshot(uint64_t peer);
  void HandleHeartbeat(const proto::Message& m);
  void HandleHeartbeatResponse(const proto::Message& m);
  void DropPendingReads();

  void Campaign();
  void BroadcastAppend();
  void SendAppend(uint64_t peer);
  void SendHeartbeat(uint64_t peer, const std::string& context = std::string());
  void MaybeAdvanceCommit();

  void Send(proto::Message message);
  void MarkHardStateDirty();

  Config config_;
  RaftLog log_;

  Role role_ = Role::kFollower;
  uint64_t term_ = 0;
  uint64_t vote_ = 0;  // 0 means "no vote this term"
  uint64_t leader_id_ = 0;
  uint64_t commit_index_ = 0;
  uint64_t applied_index_ = 0;

  std::unordered_map<uint64_t, PeerProgress> progress_;
  std::unordered_map<uint64_t, bool> votes_;
  // Peers heard from during the current CheckQuorum interval.
  std::unordered_map<uint64_t, bool> recent_active_;

  uint32_t election_elapsed_ = 0;
  uint32_t heartbeat_elapsed_ = 0;
  uint32_t randomized_election_timeout_ = 0;

  std::mt19937_64 rng_;

  struct PendingRead {
    uint64_t index = 0;
    std::set<uint64_t> acks;  // peers (including self) that confirmed leadership
  };
  std::unordered_map<std::string, PendingRead> pending_reads_;
  std::vector<ReadState> ready_reads_;
  std::vector<std::string> dropped_reads_;
  // Index of this leader's term-start no-op. A read is only safe once the
  // commit index has reached it.
  uint64_t term_start_index_ = 0;

  // Latest snapshot image, for sending to followers behind the log.
  std::string snapshot_data_;
  std::optional<InstalledSnapshot> pending_snapshot_;

  std::vector<proto::Message> pending_messages_;
  uint64_t persisted_index_ = 0;
  bool hard_state_dirty_ = false;
  proto::HardState last_hard_state_;
};

}  // namespace raftkv::raft
