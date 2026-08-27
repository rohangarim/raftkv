// Five-node Raft cluster driven inside a single test, against a simulated
// network that delays, reorders, drops, duplicates, and partitions.
//
// Every one of these runs is a pure function of its seed. When something fails
// here, the failure message carries the seed and the run replays exactly.

#include <cstdlib>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "raft_harness.h"

#include "common/log.h"

namespace raftkv::raft::testing {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::set<uint64_t> Set(std::initializer_list<uint64_t> ids) { return {ids}; }

// ---------------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------------

TEST(RaftClusterTest, ElectsExactlyOneLeader) {
  Cluster cluster(5, /*seed=*/1);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U) << "no leader was elected";
  EXPECT_EQ(cluster.Leaders().size(), 1U);
}

TEST(RaftClusterTest, ASingleNodeClusterElectsItself) {
  Cluster cluster(1, /*seed=*/2);
  const uint64_t leader = cluster.RunUntilLeader();
  EXPECT_EQ(leader, 1U) << "a lone node has a quorum of one and should not wait";
}

TEST(RaftClusterTest, NoLeaderEmergesWithoutAQuorum) {
  Cluster cluster(5, /*seed=*/3);
  ASSERT_NE(cluster.RunUntilLeader(), 0U);

  // Isolate three of five; the remaining two cannot reach a quorum.
  cluster.Crash(1);
  cluster.Crash(2);
  cluster.Crash(3);

  for (int i = 0; i < 200; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  for (uint64_t id : {4UL, 5UL}) {
    RaftNode* node = cluster.Node(id);
    ASSERT_NE(node, nullptr);
    EXPECT_NE(node->role(), Role::kLeader) << "node " << id << " became leader without a quorum";
  }
}

TEST(RaftClusterTest, ANewLeaderEmergesAfterTheOldOneDies) {
  Cluster cluster(5, /*seed=*/4);
  const uint64_t first = cluster.RunUntilLeader();
  ASSERT_NE(first, 0U);

  cluster.Crash(first);
  const uint64_t second = cluster.RunUntilLeader();
  ASSERT_NE(second, 0U) << "the cluster never recovered a leader";
  EXPECT_NE(second, first);
}

TEST(RaftClusterTest, ElectionsStillSucceedWithHeavyMessageLoss) {
  Cluster cluster(5, /*seed=*/5);
  cluster.SetDropRate(0.3);
  cluster.SetMaxDelay(3);
  EXPECT_NE(cluster.RunUntilLeader(2000), 0U)
      << "randomized timeouts should eventually win through a lossy link";
}

TEST(RaftClusterTest, DuplicatedMessagesDoNotBreakElections) {
  Cluster cluster(5, /*seed=*/6);
  cluster.SetDuplicateRate(0.5);
  EXPECT_NE(cluster.RunUntilLeader(1000), 0U);
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

TEST(RaftClusterTest, ReplicatesAProposalToEveryFollower) {
  Cluster cluster(5, /*seed=*/7);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  const std::string command = "set x = 1";
  ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes(command)).IsOk());
  cluster.DeliverAll();
  for (int i = 0; i < 20; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  for (uint64_t id : cluster.Ids()) {
    RaftNode* node = cluster.Node(id);
    ASSERT_NE(node, nullptr);
    EXPECT_GE(node->Log().LastIndex(), 2U) << "node " << id << " is missing the proposal";
  }
}

TEST(RaftClusterTest, ProposalsToAFollowerAreRefused) {
  Cluster cluster(5, /*seed=*/8);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  for (uint64_t id : cluster.Ids()) {
    if (id == leader) {
      continue;
    }
    const Status s = cluster.Node(id)->Propose(AsBytes("nope"));
    EXPECT_FALSE(s.IsOk()) << "a follower accepted a proposal";
    EXPECT_EQ(cluster.Node(id)->LeaderId(), leader) << "a follower must know where to redirect";
  }
}

TEST(RaftClusterTest, CommitIndexAdvancesOnceAQuorumHasTheEntry) {
  Cluster cluster(5, /*seed=*/9);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  const uint64_t before = cluster.Node(leader)->CommitIndex();
  ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("payload")).IsOk());
  for (int i = 0; i < 20; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  EXPECT_GT(cluster.Node(leader)->CommitIndex(), before);
}

TEST(RaftClusterTest, ManyProposalsReplicateInOrder) {
  Cluster cluster(5, /*seed=*/10);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("cmd" + std::to_string(i))).IsOk());
  }
  for (int i = 0; i < 50; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  const RaftLog& reference = cluster.Node(leader)->Log();
  for (uint64_t id : cluster.Ids()) {
    if (id == leader) {
      continue;
    }
    const RaftLog& other = cluster.Node(id)->Log();
    EXPECT_EQ(other.LastIndex(), reference.LastIndex()) << "node " << id << " lagged";
  }
}

// A follower whose log has diverged must be repaired, and the fast-backtracking
// path is what makes that take a bounded number of round trips rather than one
// per conflicting entry.
TEST(RaftClusterTest, ADivergedFollowerIsRepaired) {
  Cluster cluster(5, /*seed=*/11);
  uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  // Isolate one follower and keep committing without it.
  uint64_t victim = 0;
  for (uint64_t id : cluster.Ids()) {
    if (id != leader) {
      victim = id;
      break;
    }
  }
  std::set<uint64_t> majority;
  for (uint64_t id : cluster.Ids()) {
    if (id != victim) {
      majority.insert(id);
    }
  }
  cluster.Partition(majority);

  for (int i = 0; i < 30; ++i) {
    ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("during" + std::to_string(i))).IsOk());
    cluster.TickAll();
    cluster.DeliverAll();
  }

  cluster.Heal();
  for (int i = 0; i < 200; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  leader = cluster.SingleLeader();
  ASSERT_NE(leader, 0U);
  EXPECT_EQ(cluster.Node(victim)->Log().LastIndex(), cluster.Node(leader)->Log().LastIndex())
      << "the isolated follower never caught up after healing";
}

// ---------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------

TEST(RaftClusterTest, AMinorityPartitionCannotElectALeader) {
  Cluster cluster(5, /*seed=*/12);
  ASSERT_NE(cluster.RunUntilLeader(), 0U);

  cluster.Partition(Set({1, 2}));  // minority side
  for (int i = 0; i < 300; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  for (uint64_t id : {1UL, 2UL}) {
    EXPECT_NE(cluster.Node(id)->role(), Role::kLeader)
        << "node " << id << " led a minority partition";
  }
}

TEST(RaftClusterTest, TheMajoritySideKeepsMakingProgress) {
  Cluster cluster(5, /*seed=*/13);
  ASSERT_NE(cluster.RunUntilLeader(), 0U);

  cluster.Partition(Set({1, 2}));
  for (int i = 0; i < 300; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  const uint64_t leader = cluster.SingleLeader();
  ASSERT_NE(leader, 0U) << "the majority side failed to elect a leader";
  EXPECT_EQ(cluster.Node(leader)->Term() > 0, true);

  ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("majority write")).IsOk());
  for (int i = 0; i < 50; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  EXPECT_GT(cluster.Node(leader)->CommitIndex(), 0U);
}

TEST(RaftClusterTest, AnIsolatedLeaderStepsDownWhenTheNetworkHeals) {
  Cluster cluster(5, /*seed=*/14);
  const uint64_t old_leader = cluster.RunUntilLeader();
  ASSERT_NE(old_leader, 0U);

  cluster.Partition(Set({old_leader}));
  for (int i = 0; i < 300; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  cluster.Heal();
  for (int i = 0; i < 300; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  EXPECT_EQ(cluster.Leaders().size(), 1U) << "the cluster settled on more than one leader";
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

TEST(RaftClusterTest, ARestartedNodeRejoinsFromPersistedStateAlone) {
  Cluster cluster(5, /*seed=*/15);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes("cmd" + std::to_string(i))).IsOk());
    cluster.TickAll();
    cluster.DeliverAll();
  }

  uint64_t victim = leader == 1 ? 2 : 1;
  cluster.Crash(victim);
  for (int i = 0; i < 20; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  cluster.Restart(victim);
  for (int i = 0; i < 200; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }

  const uint64_t current = cluster.SingleLeader();
  ASSERT_NE(current, 0U);
  EXPECT_EQ(cluster.Node(victim)->Log().LastIndex(), cluster.Node(current)->Log().LastIndex())
      << "the restarted node did not catch up";
}

TEST(RaftClusterTest, RepeatedLeaderKillsNeverLoseCommittedEntries) {
  Cluster cluster(5, /*seed=*/16);

  uint64_t highest_commit = 0;
  for (int round = 0; round < 5; ++round) {
    const uint64_t leader = cluster.RunUntilLeader(500);
    ASSERT_NE(leader, 0U) << "round " << round << " never elected a leader";

    for (int i = 0; i < 5; ++i) {
      (void)cluster.Node(leader)->Propose(
          AsBytes("r" + std::to_string(round) + "-" + std::to_string(i)));
      cluster.TickAll();
      cluster.DeliverAll();
    }
    highest_commit = std::max(highest_commit, cluster.Node(leader)->CommitIndex());

    cluster.Crash(leader);
    for (int i = 0; i < 30; ++i) {
      cluster.TickAll();
      cluster.DeliverAll();
    }
    cluster.Restart(leader);
  }

  for (int i = 0; i < 300; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
  const uint64_t leader = cluster.SingleLeader();
  ASSERT_NE(leader, 0U);
  EXPECT_GE(cluster.Node(leader)->CommitIndex(), highest_commit)
      << "a committed entry was lost across leader changes";
}

// ---------------------------------------------------------------------------
// Randomized soak
// ---------------------------------------------------------------------------

// The point of this test is not any single assertion but the invariant check
// that runs after every step inside the harness. A seed that fails here is a
// complete, replayable bug report.
TEST(RaftClusterTest, RandomizedFaultsPreserveEveryInvariant) {
  // A failing seed must be replayable on demand:
  //   RAFTKV_SEED=5 ./build/test/raft_cluster_test --gtest_filter='*Randomized*'
  uint64_t first_seed = 1;
  uint64_t last_seed = 2000;
  if (const char* pinned = std::getenv("RAFTKV_SEED"); pinned != nullptr) {
    first_seed = last_seed = std::strtoull(pinned, nullptr, 10);
    // Replaying a single seed is a debugging session; turn the protocol trace
    // on so the run explains itself.
    SetLogLevel(LogLevel::kDebug);
  }

  for (uint64_t seed = first_seed; seed <= last_seed; ++seed) {
    Cluster cluster(5, seed);
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> action(0, 9);

    cluster.SetDropRate(0.05);
    cluster.SetDuplicateRate(0.05);
    cluster.SetMaxDelay(3);

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
        case 2: {
          for (uint64_t id : cluster.Ids()) {
            if (cluster.IsCrashed(id)) {
              cluster.Restart(id);
              break;
            }
          }
          break;
        }
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
        default:
          cluster.TickAll();
          cluster.DeliverAll();
          break;
      }
      if (::testing::Test::HasFatalFailure()) {
        FAIL() << "invariant violated with seed " << seed << " at step " << step;
      }
    }
  }
}

}  // namespace
}  // namespace raftkv::raft::testing
