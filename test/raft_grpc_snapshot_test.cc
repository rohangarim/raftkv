// Snapshots over real gRPC and real disk: a follower that was down while the
// leader compacted its log can only catch up by InstallSnapshot, and a node
// restarted from a snapshot must come back with its data.

#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

namespace raftkv::server {
namespace {

using testing::AsBytes;
using testing::EncodePut;
using GrpcClusterFixture = testing::GrpcClusterFixture;

class RaftGrpcSnapshotTest : public GrpcClusterFixture {
 protected:
  void SetUp() override {
    GrpcClusterFixture::SetUp();
    snapshot_threshold_ = 20;
  }

  void WaitFor(const std::function<bool()>& done, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && !done()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
};

TEST_F(RaftGrpcSnapshotTest, LeaderCompactsItsLogAfterEnoughWrites) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  for (uint64_t i = 1; i <= 60; ++i) {
    const std::string cmd = EncodePut("c", i, "k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_TRUE(nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(5))
                    .IsOk());
  }
  WaitFor([&] { return nodes_.at(leader).driver->SnapshotIndex() > 0; },
          std::chrono::milliseconds(3000));
  EXPECT_GT(nodes_.at(leader).driver->SnapshotIndex(), 0U);
}

TEST_F(RaftGrpcSnapshotTest, FollowerThatMissedACompactedLogCatchesUpBySnapshot) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  uint64_t victim = 0;
  for (auto& [id, node] : nodes_) {
    if (id != leader) {
      victim = id;
      break;
    }
  }
  CrashNode(victim);

  for (uint64_t i = 1; i <= 100; ++i) {
    const std::string cmd = EncodePut("c", i, "k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_TRUE(nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(5))
                    .IsOk());
  }
  WaitFor([&] { return nodes_.at(leader).driver->SnapshotIndex() > 0; },
          std::chrono::milliseconds(3000));
  ASSERT_GT(nodes_.at(leader).driver->SnapshotIndex(), 0U) << "leader never compacted";

  RestartNode(victim, {1, 2, 3});
  const uint64_t target = nodes_.at(leader).raw_sm->LastAppliedIndex();
  WaitFor([&] { return nodes_.at(victim).raw_sm->LastAppliedIndex() >= target; },
          std::chrono::milliseconds(8000));
  ASSERT_GE(nodes_.at(victim).raw_sm->LastAppliedIndex(), target)
      << "the follower never caught up";
  EXPECT_GT(nodes_.at(victim).driver->SnapshotIndex(), 0U)
      << "it should have caught up through a snapshot, not entries";

  for (uint64_t i = 1; i <= 100; ++i) {
    auto got = nodes_.at(victim).raw_sm->Get("k" + std::to_string(i));
    ASSERT_TRUE(got.IsOk());
    ASSERT_TRUE(got->has_value()) << "key k" << i << " missing after the snapshot install";
    EXPECT_EQ(**got, "v" + std::to_string(i));
  }
}

TEST_F(RaftGrpcSnapshotTest, NodeRestartedAfterSnapshotKeepsItsData) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  for (uint64_t i = 1; i <= 50; ++i) {
    const std::string cmd = EncodePut("c", i, "k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_TRUE(nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(5))
                    .IsOk());
  }
  WaitFor([&] { return nodes_.at(leader).driver->SnapshotIndex() > 0; },
          std::chrono::milliseconds(3000));
  ASSERT_GT(nodes_.at(leader).driver->SnapshotIndex(), 0U);

  CrashNode(leader);
  RestartNode(leader, {1, 2, 3});
  WaitFor([&] { return nodes_.at(leader).raw_sm->LastAppliedIndex() >= 50; },
          std::chrono::milliseconds(5000));
  for (uint64_t i = 1; i <= 50; ++i) {
    auto got = nodes_.at(leader).raw_sm->Get("k" + std::to_string(i));
    ASSERT_TRUE(got.IsOk());
    ASSERT_TRUE(got->has_value()) << "key k" << i << " lost across the restart";
  }
}

}  // namespace
}  // namespace raftkv::server
