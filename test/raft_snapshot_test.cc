// Log compaction and InstallSnapshot, on the same hostile simulated network as
// the cluster test. A follower that falls behind a compacted log can only catch
// up through a snapshot, so these tests make that happen on purpose.

#include <set>
#include <string>

#include <gtest/gtest.h>

#include "raft_harness.h"

namespace raftkv::raft::testing {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

void Settle(Cluster& cluster, int rounds = 60) {
  for (int i = 0; i < rounds; ++i) {
    cluster.TickAll();
    cluster.DeliverAll();
  }
}

void ProposeMany(Cluster& cluster, uint64_t leader, const std::string& prefix, int n) {
  for (int i = 0; i < n; ++i) {
    ASSERT_TRUE(cluster.Node(leader)->Propose(AsBytes(prefix + std::to_string(i))).IsOk());
    cluster.DeliverAll();
  }
  Settle(cluster, 10);
}

// A lagging follower is brought up to date by a snapshot, not by entries the
// leader no longer has.
TEST(RaftSnapshotTest, LaggingFollowerCatchesUpViaSnapshot) {
  Cluster cluster(3, /*seed=*/11);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);
  const uint64_t lagging = leader == 3 ? 2 : 3;

  cluster.Crash(lagging);
  ProposeMany(cluster, leader, "a", 20);

  // Every live node compacts, so no one still holds the early entries.
  for (uint64_t id : cluster.Ids()) {
    if (!cluster.IsCrashed(id)) {
      ASSERT_TRUE(cluster.Compact(id)) << "node " << id;
    }
  }
  ASSERT_GT(cluster.Node(leader)->Log().SnapshotIndex(), 0U);

  cluster.Restart(lagging);
  Settle(cluster);

  EXPECT_GE(cluster.SnapshotsInstalled(), 1U);
  EXPECT_EQ(cluster.Node(lagging)->CommitIndex(), cluster.Node(leader)->CommitIndex());
  EXPECT_EQ(cluster.Node(lagging)->Log().LastIndex(), cluster.Node(leader)->Log().LastIndex());
}

// After catching up by snapshot the follower keeps replicating normally.
TEST(RaftSnapshotTest, FollowerKeepsReplicatingAfterInstall) {
  Cluster cluster(3, /*seed=*/12);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);
  const uint64_t lagging = leader == 3 ? 2 : 3;

  cluster.Crash(lagging);
  ProposeMany(cluster, leader, "a", 15);
  for (uint64_t id : cluster.Ids()) {
    if (!cluster.IsCrashed(id)) cluster.Compact(id);
  }
  cluster.Restart(lagging);
  Settle(cluster);
  ProposeMany(cluster, leader, "b", 10);
  Settle(cluster);

  EXPECT_EQ(cluster.Node(lagging)->CommitIndex(), cluster.Node(leader)->CommitIndex());
  EXPECT_EQ(cluster.Node(lagging)->Log().LastIndex(), cluster.Node(leader)->Log().LastIndex());
}

// A node restarted from a snapshot plus the log after it rejoins cleanly.
TEST(RaftSnapshotTest, RestartFromSnapshotAndLogSuffix) {
  Cluster cluster(3, /*seed=*/13);
  const uint64_t leader = cluster.RunUntilLeader();
  ASSERT_NE(leader, 0U);
  ProposeMany(cluster, leader, "a", 12);
  for (uint64_t id : cluster.Ids()) {
    ASSERT_TRUE(cluster.Compact(id));
  }
  ProposeMany(cluster, leader, "b", 6);

  const uint64_t follower = leader == 1 ? 2 : 1;
  cluster.Crash(follower);
  cluster.Restart(follower);
  Settle(cluster);
  ProposeMany(cluster, leader, "c", 4);
  Settle(cluster);

  EXPECT_EQ(cluster.Node(follower)->CommitIndex(), cluster.Node(leader)->CommitIndex());
}

// Randomised: compaction, crashes and message loss interleaved. The cluster's
// invariants (including State Machine Safety over snapshot-transferred state)
// are checked after every step, so any divergence fails at its cause.
TEST(RaftSnapshotTest, RandomizedCompactionAndCrashes) {
  uint64_t installs = 0;
  for (uint64_t seed = 100; seed < 300; ++seed) {
    Cluster cluster(5, seed);
    cluster.SetDropRate(0.05);
    cluster.SetDuplicateRate(0.05);
    cluster.SetMaxDelay(3);
    std::mt19937_64 rng(seed);

    for (int step = 0; step < 250; ++step) {
      cluster.TickAll();
      cluster.DeliverAll(50);
      const uint64_t leader = cluster.SingleLeader();
      const uint64_t roll = rng() % 20;
      if (leader != 0 && roll < 10) {
        (void)cluster.Node(leader)->Propose(AsBytes("s" + std::to_string(seed) + "-" +
                                                    std::to_string(step)));
      } else if (roll < 13) {
        cluster.Compact(cluster.Ids()[rng() % cluster.Ids().size()]);
      } else if (roll == 13) {
        const uint64_t victim = cluster.Ids()[rng() % cluster.Ids().size()];
        if (!cluster.IsCrashed(victim)) cluster.Crash(victim);
      } else if (roll == 14) {
        const uint64_t id = cluster.Ids()[rng() % cluster.Ids().size()];
        if (cluster.IsCrashed(id)) cluster.Restart(id);
      }
      if (::testing::Test::HasFatalFailure()) {
        return;
      }
    }
    installs += cluster.SnapshotsInstalled();
  }
  EXPECT_GT(installs, 0U) << "the run never exercised InstallSnapshot";
}

}  // namespace
}  // namespace raftkv::raft::testing
