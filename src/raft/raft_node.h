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
#include <random>
#include <span>
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

  bool Empty() const {
    return !hard_state.has_value() && entries_to_persist.empty() && messages.empty() &&
           committed_entries.empty();
  }
};

class RaftNode {
 public:
  explicit RaftNode(Config config);

  // Restores durable state after a restart. Everything else -- role, commit
  // index, leader -- is volatile and rebuilt by the protocol.
  void RestoreHardState(const proto::HardState& hard_state);
  Status RestoreLog(std::span<const proto::Entry> entries);

  void Step(const proto::Message& message);
  void Tick();

  // Appends to the leader's log. Fails if this node is not the leader; the
  // caller is expected to redirect using LeaderId().
  Status Propose(std::span<const std::byte> command);

  Ready ReadyToProcess();
  void Advance(const Ready& ready);

  uint64_t Id() const { return config_.id; }
  Role role() const { return role_; }
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
  void HandleHeartbeat(const proto::Message& m);

  void Campaign();
  void BroadcastAppend();
  void SendAppend(uint64_t peer);
  void SendHeartbeat(uint64_t peer);
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

  std::vector<proto::Message> pending_messages_;
  uint64_t persisted_index_ = 0;
  bool hard_state_dirty_ = false;
  proto::HardState last_hard_state_;
};

}  // namespace raftkv::raft
