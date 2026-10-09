// Linearizable reads (ReadIndex).
//
// Two layers. The first drives a single RaftNode by hand so each protocol
// detail -- the own-term-commit gate, the request-specific ack, dropping reads
// on step-down -- is pinned down individually. The second runs whole clusters
// in the simulator, where the invariant is the real one: a read point must
// never be below an index that was already committed when the read began.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "raft_harness.h"

#include "raft/raft_node.h"

namespace raftkv::raft {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// ---------------------------------------------------------------------------
// One node, driven by hand.
// ---------------------------------------------------------------------------

Ready Drain(RaftNode& node) {
  Ready ready = node.ReadyToProcess();
  node.Advance(ready);
  return ready;
}

proto::Message Msg(proto::MessageType type, uint64_t from, uint64_t to, uint64_t term) {
  proto::Message m;
  m.set_type(type);
  m.set_from(from);
  m.set_to(to);
  m.set_term(term);
  return m;
}

Config ThreeNodeConfig() {
  Config c;
  c.id = 1;
  c.peers = {1, 2, 3};
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  c.random_seed = 7;
  return c;
}

// Drives node 1 to leader of {1,2,3} with its term-start no-op NOT yet
// committed. The vote is granted by hand so nothing else can sneak in.
void ElectWithoutCommit(RaftNode& node) {
  for (int i = 0; i < 100 && node.CurrentRole() != Role::kCandidate; ++i) {
    node.Tick();
  }
  ASSERT_EQ(node.CurrentRole(), Role::kCandidate);
  Drain(node);

  proto::Message vote = Msg(proto::MSG_REQUEST_VOTE_RESP, 2, 1, node.Term());
  vote.set_reject(false);
  node.Step(vote);
  ASSERT_EQ(node.CurrentRole(), Role::kLeader);
  Drain(node);
}

// Follower 2 acknowledges everything the leader has, which commits the no-op.
void CommitTermStartEntry(RaftNode& node) {
  proto::Message ack = Msg(proto::MSG_APPEND_RESP, 2, 1, node.Term());
  ack.set_reject(false);
  ack.set_index(node.Log().LastIndex());
  node.Step(ack);
  Drain(node);
}

std::vector<proto::Message> HeartbeatsWithContext(const Ready& ready, const std::string& ctx) {
  std::vector<proto::Message> out;
  for (const auto& m : ready.messages) {
    if (m.type() == proto::MSG_HEARTBEAT && m.context() == ctx) {
      out.push_back(m);
    }
  }
  return out;
}

TEST(ReadIndexTest, AFollowerRefusesAndPointsAtTheLeader) {
  RaftNode node(ThreeNodeConfig());
  const Status s = node.ReadIndex("r");
  EXPECT_EQ(s.ErrCode(), lsm::Code::kNotSupported);
}

// A new leader's commit index can lag what the previous leader already told
// clients was committed. Serving a read at that index would be stale, so reads
// wait for the term-start no-op to commit -- the Figure 8 hazard, for reads.
TEST(ReadIndexTest, ALeaderThatHasNotCommittedInItsTermRefuses) {
  RaftNode node(ThreeNodeConfig());
  ElectWithoutCommit(node);

  const Status s = node.ReadIndex("r");
  EXPECT_EQ(s.ErrCode(), lsm::Code::kUnavailable) << s.ToString();

  CommitTermStartEntry(node);
  EXPECT_TRUE(node.ReadIndex("r").IsOk());
}

TEST(ReadIndexTest, NeedsAQuorumOfAcksForThisRequest) {
  RaftNode node(ThreeNodeConfig());
  ElectWithoutCommit(node);
  CommitTermStartEntry(node);
  const uint64_t commit_at_request = node.CommitIndex();

  ASSERT_TRUE(node.ReadIndex("r1").IsOk());
  Ready sent = Drain(node);
  EXPECT_EQ(HeartbeatsWithContext(sent, "r1").size(), 2U)
      << "the read must be confirmed by fresh heartbeats to every peer";
  EXPECT_TRUE(sent.read_states.empty()) << "served before any quorum confirmation";

  // An ack with no context proves liveness at some earlier moment, not
  // leadership at the moment of this request.
  proto::Message plain = Msg(proto::MSG_HEARTBEAT_RESP, 2, 1, node.Term());
  node.Step(plain);
  EXPECT_TRUE(Drain(node).read_states.empty());

  // An ack for a different read does not count either.
  proto::Message other = Msg(proto::MSG_HEARTBEAT_RESP, 2, 1, node.Term());
  other.set_context("some-other-read");
  node.Step(other);
  EXPECT_TRUE(Drain(node).read_states.empty());

  proto::Message ack = Msg(proto::MSG_HEARTBEAT_RESP, 2, 1, node.Term());
  ack.set_context("r1");
  node.Step(ack);
  Ready done = Drain(node);
  ASSERT_EQ(done.read_states.size(), 1U);
  EXPECT_EQ(done.read_states[0].context, "r1");
  EXPECT_EQ(done.read_states[0].index, commit_at_request);
}

// The read point is the commit index when the read STARTED. Entries committed
// while waiting for the quorum do not move it; the read is still correct
// because everything committed before it began is at or below that index.
TEST(ReadIndexTest, ReadPointIsFixedAtRequestTime) {
  RaftNode node(ThreeNodeConfig());
  ElectWithoutCommit(node);
  CommitTermStartEntry(node);
  const uint64_t at_request = node.CommitIndex();

  ASSERT_TRUE(node.ReadIndex("r").IsOk());
  Drain(node);

  ASSERT_TRUE(node.Propose(AsBytes("later")).IsOk());
  Drain(node);
  proto::Message append_ack = Msg(proto::MSG_APPEND_RESP, 2, 1, node.Term());
  append_ack.set_reject(false);
  append_ack.set_index(node.Log().LastIndex());
  node.Step(append_ack);
  Drain(node);
  ASSERT_GT(node.CommitIndex(), at_request);

  proto::Message ack = Msg(proto::MSG_HEARTBEAT_RESP, 3, 1, node.Term());
  ack.set_context("r");
  node.Step(ack);
  Ready done = Drain(node);
  ASSERT_EQ(done.read_states.size(), 1U);
  EXPECT_EQ(done.read_states[0].index, at_request);
}

TEST(ReadIndexTest, RejectsEmptyAndDuplicateContexts) {
  RaftNode node(ThreeNodeConfig());
  ElectWithoutCommit(node);
  CommitTermStartEntry(node);

  EXPECT_EQ(node.ReadIndex("").ErrCode(), lsm::Code::kInvalidArgument);
  ASSERT_TRUE(node.ReadIndex("dup").IsOk());
  EXPECT_EQ(node.ReadIndex("dup").ErrCode(), lsm::Code::kInvalidArgument);
}

TEST(ReadIndexTest, SteppingDownDropsPendingReadsAndLateAcksAreIgnored) {
  RaftNode node(ThreeNodeConfig());
  ElectWithoutCommit(node);
  CommitTermStartEntry(node);

  ASSERT_TRUE(node.ReadIndex("doomed").IsOk());
  Drain(node);

  // A heartbeat from a newer term deposes this leader.
  proto::Message newer = Msg(proto::MSG_HEARTBEAT, 3, 1, node.Term() + 1);
  node.Step(newer);
  ASSERT_EQ(node.CurrentRole(), Role::kFollower);
  Ready after = Drain(node);
  ASSERT_EQ(after.dropped_reads.size(), 1U);
  EXPECT_EQ(after.dropped_reads[0], "doomed");

  proto::Message late = Msg(proto::MSG_HEARTBEAT_RESP, 2, 1, node.Term());
  late.set_context("doomed");
  node.Step(late);
  EXPECT_TRUE(Drain(node).read_states.empty()) << "a deposed leader served a read";
}

TEST(ReadIndexTest, ASingleNodeClusterServesReadsImmediately) {
  Config c;
  c.id = 1;
  c.peers = {1};
  c.random_seed = 3;
  RaftNode node(c);
  for (int i = 0; i < 100 && node.CurrentRole() != Role::kLeader; ++i) {
    node.Tick();
  }
  ASSERT_EQ(node.CurrentRole(), Role::kLeader);
  Drain(node);

  // The no-op commits on its own: a lone leader's log is already a quorum.
  ASSERT_TRUE(node.ReadIndex("solo").IsOk());
  Ready ready = Drain(node);
  ASSERT_EQ(ready.read_states.size(), 1U);
  EXPECT_EQ(ready.read_states[0].index, node.CommitIndex());
}

// ---------------------------------------------------------------------------
// Whole clusters, in the simulator.
// ---------------------------------------------------------------------------

using testing::Cluster;

uint64_t OtherLeader(Cluster& cluster, uint64_t excluding) {
  for (uint64_t id : cluster.Leaders()) {
    if (id != excluding) {
      return id;
    }
  }
  return 0;
}

TEST(ReadIndexClusterTest, AHealthyLeaderServesARead) {
  Cluster cluster(5, /*seed=*/21);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);
  ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("w1")).IsOk());
  for (int i = 0; i < 30; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  ASSERT_TRUE(cluster.RequestRead(leader, "healthy").IsOk());
  cluster.DeliverAll();
  ASSERT_TRUE(cluster.WasServed("healthy"));
  EXPECT_GE(cluster.ServedIndex("healthy"), 2U) << "the read does not cover the write";
}

// The Phase 4 bar: a partitioned old leader cannot serve a stale read.
TEST(ReadIndexClusterTest, APartitionedOldLeaderCannotServeAStaleRead) {
  Cluster cluster(5, /*seed=*/22);
  const uint64_t old_leader = cluster.RunUntilLeader();
  ASSERT_NE(old_leader, 0U);
  ASSERT_TRUE(cluster.Node(old_leader)->Propose(AsBytes("before")).IsOk());
  for (int i = 0; i < 30; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  // Cut the old leader off, then ask it for a read while it still believes it
  // is leader. It cannot reach a quorum, so it must not answer.
  cluster.Partition({old_leader});
  ASSERT_TRUE(cluster.RequestRead(old_leader, "stale").IsOk());
  cluster.DeliverAll();
  EXPECT_FALSE(cluster.WasServed("stale")) << "an isolated leader served a read";

  // Meanwhile the majority elects a new leader and commits a newer write.
  uint64_t new_leader = 0;
  for (int i = 0; i < 500 && new_leader == 0; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
    new_leader = OtherLeader(cluster, old_leader);
  }
  ASSERT_NE(new_leader, 0U) << "the majority never elected a leader";
  ASSERT_TRUE(cluster.Node(new_leader)->Propose(AsBytes("after")).IsOk());
  for (int i = 0; i < 60; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  const uint64_t committed_after = cluster.Node(new_leader)->CommitIndex();

  EXPECT_FALSE(cluster.WasServed("stale"))
      << "the old leader eventually answered a read it could not have confirmed";
  EXPECT_TRUE(cluster.WasDropped("stale"))
      << "the old leader should have failed the read when it stepped down";

  // And the cluster still serves reads, at a point that includes the new write.
  cluster.Heal();
  for (int i = 0; i < 60; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  const uint64_t leader_now = cluster.SingleLeader();
  ASSERT_NE(leader_now, 0U);
  ASSERT_TRUE(cluster.RequestRead(leader_now, "fresh").IsOk());
  cluster.DeliverAll();
  ASSERT_TRUE(cluster.WasServed("fresh"));
  EXPECT_GE(cluster.ServedIndex("fresh"), committed_after);
}

// Reads are issued against EVERY node that believes it is leader, stale ones
// included, while partitions, crashes and message loss churn the cluster. The
// harness fails the step on which any read point falls below an already
// committed index.
TEST(ReadIndexClusterTest, RandomizedFaultsNeverProduceAStaleRead) {
  size_t total_served = 0;
  for (uint64_t seed = 1; seed <= 400; ++seed) {
    Cluster cluster(5, seed);
    std::mt19937_64 rng(seed * 7919);
    std::uniform_int_distribution<int> action(0, 11);
    cluster.SetDropRate(0.05);
    cluster.SetDuplicateRate(0.05);
    cluster.SetMaxDelay(3);

    int next_read = 0;
    for (int step = 0; step < 300; ++step) {
      switch (action(rng)) {
        case 0: {
          const uint64_t leader = cluster.SingleLeader();
          if (leader != 0) {
            (void)cluster.Node(leader)->Propose(AsBytes("s" + std::to_string(step)));
          }
          break;
        }
        case 1: {
          std::uniform_int_distribution<size_t> pick(0, cluster.Ids().size() - 1);
          const uint64_t victim = cluster.Ids()[pick(rng)];
          if (!cluster.IsCrashed(victim)) {
            cluster.Crash(victim);
          }
          break;
        }
        case 2:
          for (uint64_t id : cluster.Ids()) {
            if (cluster.IsCrashed(id)) {
              cluster.Restart(id);
              break;
            }
          }
          break;
        case 3: {
          std::set<uint64_t> side;
          std::uniform_int_distribution<int> coin(0, 1);
          for (uint64_t id : cluster.Ids()) {
            if (coin(rng) == 1) {
              side.insert(id);
            }
          }
          cluster.Partition(side);
          break;
        }
        case 4:
          cluster.Heal();
          break;
        case 5:
        case 6: {
          const auto leaders = cluster.Leaders();
          if (!leaders.empty()) {
            std::uniform_int_distribution<size_t> pick(0, leaders.size() - 1);
            (void)cluster.RequestRead(leaders[pick(rng)], "r" + std::to_string(next_read++));
          }
          break;
        }
        default:
          cluster.TickAll();
          cluster.DeliverAll();
          break;
      }
      if (::testing::Test::HasFatalFailure()) {
        FAIL() << "stale read with seed " << seed << " at step " << step;
      }
    }
    total_served += cluster.TotalServedReads();
  }
  EXPECT_GT(total_served, 0U) << "no read was ever served, so the check proved nothing";
}

}  // namespace
}  // namespace raftkv::raft
