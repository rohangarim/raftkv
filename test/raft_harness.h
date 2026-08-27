#pragma once

// A deterministic, deliberately hostile simulated network.
//
// The whole cluster runs inside one thread. Every message is delivered by an
// explicit step, so a run is a pure function of its seed -- which means a
// failure is a reproducible bug report, not an anecdote. The five Raft safety
// invariants are checked after EVERY step, so a violation is caught at the
// step that caused it rather than thousands of steps later when it finally
// shows up as bad data.
//
// The network can delay, reorder, drop, and duplicate messages, and can
// partition arbitrary subsets. Nodes can be crashed and restarted with only
// their persisted state, which is exactly the durability contract the
// Ready/Advance protocol defines.

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "raft/raft_node.h"

namespace raftkv::raft::testing {

// What a node keeps across a crash. Everything else is volatile and rebuilt by
// the protocol; if the cluster cannot recover from just this, the design is
// wrong.
struct PersistentState {
  proto::HardState hard_state;
  std::vector<proto::Entry> log;
};

struct InFlight {
  proto::Message message;
  uint64_t deliver_at_step = 0;
};

class Cluster {
 public:
  Cluster(size_t node_count, uint64_t seed) : seed_(seed), rng_(seed) {
    for (uint64_t id = 1; id <= node_count; ++id) {
      ids_.push_back(id);
    }
    for (uint64_t id : ids_) {
      persisted_[id] = PersistentState{};
      Start(id);
    }
  }

  // ---- topology ----------------------------------------------------------

  void Partition(const std::set<uint64_t>& side_a) {
    partition_a_ = side_a;
    partitioned_ = true;
  }
  void Heal() {
    partitioned_ = false;
    partition_a_.clear();
  }
  bool Reachable(uint64_t from, uint64_t to) const {
    if (crashed_.count(from) != 0 || crashed_.count(to) != 0) {
      return false;
    }
    if (!partitioned_) {
      return true;
    }
    return partition_a_.count(from) == partition_a_.count(to);
  }

  // ---- lifecycle ---------------------------------------------------------

  void Crash(uint64_t id) {
    crashed_.insert(id);
    nodes_.erase(id);
    // In-flight messages to a crashed node are lost, as they would be.
    std::erase_if(network_, [id](const InFlight& f) { return f.message.to() == id; });
  }

  // Restarts from persisted state only.
  void Restart(uint64_t id) {
    crashed_.erase(id);
    Start(id);
  }

  bool IsCrashed(uint64_t id) const { return crashed_.count(id) != 0; }

  // ---- driving -----------------------------------------------------------

  void TickAll() {
    for (uint64_t id : ids_) {
      if (crashed_.count(id) == 0) {
        nodes_[id]->Tick();
      }
    }
    DrainReady();
  }

  void TickNode(uint64_t id) {
    if (crashed_.count(id) == 0) {
      nodes_[id]->Tick();
      DrainReady();
    }
  }

  // Delivers one due message, if any. Returns false when nothing was ready.
  bool DeliverOne() {
    auto it = std::find_if(network_.begin(), network_.end(),
                           [this](const InFlight& f) { return f.deliver_at_step <= step_; });
    if (it == network_.end()) {
      return false;
    }
    const proto::Message m = it->message;
    network_.erase(it);

    if (crashed_.count(m.to()) == 0 && Reachable(m.from(), m.to())) {
      nodes_[m.to()]->Step(m);
    }
    DrainReady();
    return true;
  }

  // Delivers everything currently deliverable, plus whatever that produces.
  void DeliverAll(size_t max_rounds = 10000) {
    for (size_t i = 0; i < max_rounds; ++i) {
      ++step_;
      if (!DeliverOne()) {
        return;
      }
    }
  }

  // Ticks and delivers until a leader exists, or gives up.
  uint64_t RunUntilLeader(size_t max_ticks = 500) {
    for (size_t i = 0; i < max_ticks; ++i) {
      TickAll();
      DeliverAll();
      const uint64_t leader = SingleLeader();
      if (leader != 0) {
        return leader;
      }
    }
    return 0;
  }

  // ---- inspection --------------------------------------------------------

  RaftNode* Node(uint64_t id) {
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : it->second.get();
  }

  // A leader that a quorum currently agrees on, or 0.
  uint64_t SingleLeader() const {
    uint64_t found = 0;
    size_t count = 0;
    for (const auto& [id, node] : nodes_) {
      if (node->role() == Role::kLeader) {
        found = id;
        ++count;
      }
    }
    return count == 1 ? found : 0;
  }

  std::vector<uint64_t> Leaders() const {
    std::vector<uint64_t> out;
    for (const auto& [id, node] : nodes_) {
      if (node->role() == Role::kLeader) {
        out.push_back(id);
      }
    }
    return out;
  }

  const std::vector<uint64_t>& Ids() const { return ids_; }
  uint64_t Seed() const { return seed_; }

  const std::vector<proto::Entry>& PersistedLog(uint64_t id) const { return persisted_.at(id).log; }

  // ---- fault injection ---------------------------------------------------

  void SetDropRate(double rate) { drop_rate_ = rate; }
  void SetDuplicateRate(double rate) { duplicate_rate_ = rate; }
  void SetMaxDelay(uint64_t steps) { max_delay_ = steps; }

  // ---- invariants --------------------------------------------------------

  // Checked after every step. Each failure names the seed so the run can be
  // replayed exactly.
  void CheckInvariants();

 private:
  void Start(uint64_t id) {
    Config cfg;
    cfg.id = id;
    cfg.peers = ids_;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;
    // Distinct per node, derived from the seed, so nodes do not time out in
    // lockstep yet the run stays reproducible.
    cfg.random_seed = seed_ * 1000 + id;
    auto node = std::make_unique<RaftNode>(cfg);

    const PersistentState& state = persisted_[id];
    node->RestoreHardState(state.hard_state);
    if (!state.log.empty()) {
      // Never swallow this. A failed restore leaves a partially-populated log
      // that looks like a consensus bug several hundred steps later.
      const Status s = node->RestoreLog(state.log);
      ASSERT_TRUE(s.IsOk()) << "node " << id << " failed to restore its log: " << s.ToString()
                            << " [seed=" << seed_ << "]";
    }
    nodes_[id] = std::move(node);
  }

  // Pulls Ready from every node and honours the durability contract: persist
  // hard state and entries FIRST, then release messages.
  void DrainReady() {
    for (uint64_t id : ids_) {
      if (crashed_.count(id) != 0) {
        continue;
      }
      RaftNode* node = nodes_[id].get();
      Ready ready = node->ReadyToProcess();
      if (ready.Empty()) {
        continue;
      }

      if (ready.hard_state.has_value()) {
        persisted_[id].hard_state = *ready.hard_state;
      }
      for (const auto& entry : ready.entries_to_persist) {
        auto& log = persisted_[id].log;
        // This vector models a WAL: position i holds log index i+1. Overwrite
        // on conflict, the way a real WAL truncates. The check matters -- a
        // gap here silently produces a restored log with wrong indices, which
        // then fails an invariant that has nothing to do with the bug.
        const size_t pos = entry.index() - 1;
        if (pos < log.size()) {
          log.resize(pos);
        }
        ASSERT_EQ(log.size(), pos)
            << "harness WAL model: node " << id << " left a hole before index " << entry.index()
            << " [seed=" << seed_ << "]";
        log.push_back(entry);
      }

      for (const auto& applied : ready.committed_entries) {
        applied_[id][applied.index()] = applied.SerializeAsString();
      }

      for (const auto& m : ready.messages) {
        Enqueue(m);
      }
      node->Advance(ready);
    }
    CheckInvariants();
  }

  void Enqueue(const proto::Message& m) {
    std::uniform_real_distribution<double> chance(0.0, 1.0);
    if (chance(rng_) < drop_rate_) {
      return;
    }
    std::uniform_int_distribution<uint64_t> delay(0, max_delay_);
    network_.push_back({m, step_ + delay(rng_)});
    if (chance(rng_) < duplicate_rate_) {
      network_.push_back({m, step_ + delay(rng_)});
    }
  }

  uint64_t seed_;
  std::mt19937_64 rng_;
  std::vector<uint64_t> ids_;
  std::map<uint64_t, std::unique_ptr<RaftNode>> nodes_;
  std::map<uint64_t, PersistentState> persisted_;
  // node -> index -> serialized entry, for State Machine Safety.
  std::map<uint64_t, std::map<uint64_t, std::string>> applied_;
  std::set<uint64_t> crashed_;
  std::vector<InFlight> network_;

  bool partitioned_ = false;
  std::set<uint64_t> partition_a_;

  double drop_rate_ = 0.0;
  double duplicate_rate_ = 0.0;
  uint64_t max_delay_ = 0;
  uint64_t step_ = 0;

  // Highest term observed with a leader, for Election Safety.
  std::map<uint64_t, uint64_t> leader_by_term_;
  // node -> last observed log, for Leader Append-Only.
  std::map<uint64_t, std::vector<std::string>> last_leader_log_;
};

}  // namespace raftkv::raft::testing
