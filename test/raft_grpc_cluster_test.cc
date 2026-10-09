// A real 3-node Raft cluster: real gRPC servers on 127.0.0.1, real disk, real
// KvStateMachine. This is the Phase 3 "done when" bar: elect a leader,
// replicate writes, survive the leader going away, recover with no lost
// acknowledged writes.
//
// Recovery here is validated by destroying and recreating a node's driver
// against the SAME on-disk directory -- not by sending SIGKILL to a real OS
// process. That proves the persistence contract (everything acknowledged is
// on disk when a crash could plausibly happen) but not signal-safety under an
// asynchronous real kill; that gap is real and is recorded in the README
// limitations rather than implied away.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

namespace raftkv::server {
namespace {

using testing::AsBytes;
using testing::EncodePut;
using GrpcClusterFixture = testing::GrpcClusterFixture;

class RaftGrpcClusterTest : public GrpcClusterFixture {};

TEST_F(RaftGrpcClusterTest, ElectsALeaderOverRealGrpc) {
  StartCluster({1, 2, 3});
  EXPECT_NE(WaitForLeader(), 0U) << "no leader was elected over real gRPC";
}

TEST_F(RaftGrpcClusterTest, ReplicatesAWriteToEveryFollowersDisk) {
  StartCluster({1, 2, 3});
  const uint64_t leader_id = WaitForLeader();
  ASSERT_NE(leader_id, 0U);

  const std::string cmd = EncodePut("client-1", 1, "hello", "world");
  ASSERT_TRUE(nodes_.at(leader_id).driver->Propose(AsBytes(cmd)).IsOk());

  WaitForAppliedAtLeast(2, std::chrono::milliseconds(2000));  // noop + this proposal

  for (auto& [id, node] : nodes_) {
    auto got = node.raw_sm->Get("hello");
    ASSERT_TRUE(got.IsOk()) << "node " << id << ": " << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << "node " << id << " never applied the write";
    EXPECT_EQ(**got, "world") << "node " << id;
  }
}

TEST_F(RaftGrpcClusterTest, ANewLeaderIsElectedAfterTheOldOneCrashes) {
  StartCluster({1, 2, 3});
  const uint64_t first_leader = WaitForLeader();
  ASSERT_NE(first_leader, 0U);

  CrashNode(first_leader);

  const uint64_t second_leader = WaitForLeader(std::chrono::milliseconds(5000));
  ASSERT_NE(second_leader, 0U) << "no new leader emerged after the old one crashed";
  EXPECT_NE(second_leader, first_leader);
}

// The Phase 3 "done when" bar: writes acknowledged before a crash are not
// lost, verified by tearing down a node's whole process-level state and
// rebuilding it against the same directory.
TEST_F(RaftGrpcClusterTest, NoAcknowledgedWriteIsLostAcrossACrashAndRestart) {
  StartCluster({1, 2, 3});
  uint64_t leader_id = WaitForLeader();
  ASSERT_NE(leader_id, 0U);

  for (int i = 0; i < 5; ++i) {
    const std::string cmd = EncodePut("client-1", static_cast<uint64_t>(i) + 1,
                                      "k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_TRUE(nodes_.at(leader_id).driver->Propose(AsBytes(cmd)).IsOk());
  }
  WaitForAppliedAtLeast(6, std::chrono::milliseconds(2000));

  // Crash a FOLLOWER (not the leader) so the cluster keeps running and we can
  // prove the crashed node catches back up to what was already committed.
  uint64_t victim = 0;
  for (auto& [id, node] : nodes_) {
    if (id != leader_id) {
      victim = id;
      break;
    }
  }
  ASSERT_NE(victim, 0U);

  CrashNode(victim);
  RestartNode(victim, {1, 2, 3});

  // The restarted node must recover its pre-crash state from disk AND catch
  // up on whatever it missed while it was down.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
  bool caught_up = false;
  while (std::chrono::steady_clock::now() < deadline) {
    if (nodes_.at(victim).raw_sm->LastAppliedIndex() >= 6) {
      caught_up = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(caught_up) << "the restarted node never caught up";

  for (int i = 0; i < 5; ++i) {
    auto got = nodes_.at(victim).raw_sm->Get("k" + std::to_string(i));
    ASSERT_TRUE(got.IsOk());
    ASSERT_TRUE(got->has_value()) << "restarted node lost key k" << i;
    EXPECT_EQ(**got, "v" + std::to_string(i));
  }
}

// ---------------------------------------------------------------------------
// Acknowledged writes and linearizable reads, through the driver.
// ---------------------------------------------------------------------------

std::string EncodeCas(const std::string& client, uint64_t seq, const std::string& key,
                      const std::optional<std::string>& expected, const std::string& value) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* cas = c.mutable_cas();
  cas->set_key(key);
  if (expected.has_value()) {
    cas->set_expected(*expected);
  }
  cas->set_new_value(value);
  return c.SerializeAsString();
}

constexpr auto kWait = std::chrono::milliseconds(3000);

TEST_F(RaftGrpcClusterTest, ProposeAndWaitReturnsTheStateMachinesAnswer) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  RaftDriver& driver = *nodes_.at(leader).driver;

  const std::string put = EncodePut("c1", 1, "k", "v");
  auto put_result = driver.ProposeAndWait(AsBytes(put), kWait);
  ASSERT_TRUE(put_result.IsOk()) << put_result.GetStatus().ToString();
  EXPECT_TRUE(put_result->status.IsOk());
  EXPECT_FALSE(put_result->deduplicated);

  // A failed comparison is a successful apply that reports a negative answer.
  const std::string cas = EncodeCas("c1", 2, "k", std::string("wrong"), "x");
  auto cas_result = driver.ProposeAndWait(AsBytes(cas), kWait);
  ASSERT_TRUE(cas_result.IsOk()) << cas_result.GetStatus().ToString();
  EXPECT_TRUE(cas_result->status.IsOk());
  EXPECT_TRUE(cas_result->cas_mismatch);
}

// The same (client_id, seq) arriving twice is what a client retry after a
// timeout looks like. It must apply once and return the original answer.
TEST_F(RaftGrpcClusterTest, ARetriedCommandIsAppliedOnce) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  RaftDriver& driver = *nodes_.at(leader).driver;

  const std::string create = EncodeCas("c1", 1, "k", std::nullopt, "created");
  auto first = driver.ProposeAndWait(AsBytes(create), kWait);
  ASSERT_TRUE(first.IsOk());
  EXPECT_FALSE(first->deduplicated);
  EXPECT_FALSE(first->cas_mismatch);

  auto retry = driver.ProposeAndWait(AsBytes(create), kWait);
  ASSERT_TRUE(retry.IsOk());
  EXPECT_TRUE(retry->deduplicated) << "the retry was applied a second time";
  EXPECT_FALSE(retry->cas_mismatch)
      << "a re-applied create-if-absent would report a mismatch, proving double application";
}

TEST_F(RaftGrpcClusterTest, AFollowerRefusesToProposeAndKnowsTheLeader) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  uint64_t follower = 0;
  for (auto& [id, node] : nodes_) {
    if (id != leader) {
      follower = id;
      break;
    }
  }
  // Followers learn the leader from its first heartbeat.
  const auto deadline = std::chrono::steady_clock::now() + kWait;
  while (nodes_.at(follower).driver->LeaderHint() != leader &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_EQ(nodes_.at(follower).driver->LeaderHint(), leader);
  EXPECT_FALSE(nodes_.at(follower).driver->AddressOf(leader).empty());

  const std::string cmd = EncodePut("c1", 1, "k", "v");
  auto result = nodes_.at(follower).driver->ProposeAndWait(AsBytes(cmd), kWait);
  ASSERT_FALSE(result.IsOk());
  EXPECT_EQ(result.GetStatus().ErrCode(), lsm::Code::kNotSupported);

  auto read = nodes_.at(follower).driver->LinearizableGet("k", kWait);
  ASSERT_FALSE(read.IsOk());
  EXPECT_EQ(read.GetStatus().ErrCode(), lsm::Code::kNotSupported);
}

TEST_F(RaftGrpcClusterTest, AnAcknowledgedWriteIsVisibleToALinearizableRead) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  RaftDriver& driver = *nodes_.at(leader).driver;

  for (int i = 0; i < 20; ++i) {
    const std::string key = "key" + std::to_string(i);
    const std::string value = "value" + std::to_string(i);
    const std::string cmd = EncodePut("c1", static_cast<uint64_t>(i) + 1, key, value);
    ASSERT_TRUE(driver.ProposeAndWait(AsBytes(cmd), kWait).IsOk());

    auto got = driver.LinearizableGet(key, kWait);
    ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << "read missed a write that was already acknowledged";
    EXPECT_EQ(**got, value);
  }

  auto missing = driver.LinearizableGet("never-written", kWait);
  ASSERT_TRUE(missing.IsOk());
  EXPECT_FALSE(missing->has_value());
}

// A read on an idle cluster is pinned to the term-start no-op, which the state
// machine skips. If the driver waited on the state machine's own applied index
// this would hang forever (DECISIONS.md D-0037).
TEST_F(RaftGrpcClusterTest, AReadOnAnIdleClusterDoesNotWaitForeverOnANoop) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  // Let the term-start no-op commit with no client write behind it.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto got = nodes_.at(leader).driver->LinearizableGet("nothing", kWait);
  ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
  EXPECT_FALSE(got->has_value());
}

// Many writers at once means many AppendEntries handlers running concurrently
// on every follower, each draining its own slice of committed entries. If apply
// order could differ from commit order, a follower would mark a later entry
// applied and then discard an earlier one as a duplicate.
TEST_F(RaftGrpcClusterTest, ConcurrentWritersLeaveEveryNodeWithIdenticalState) {
  election_tick_ = 60;
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  RaftDriver& driver = *nodes_.at(leader).driver;

  constexpr int kThreads = 8;
  constexpr int kPerThread = 40;
  std::atomic<int> failures{0};
  std::mutex first_failure_mutex;
  std::string first_failure;
  std::vector<std::thread> writers;
  writers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t] {
      const std::string client = "writer" + std::to_string(t);
      for (int i = 0; i < kPerThread; ++i) {
        const std::string cmd =
            EncodePut(client, static_cast<uint64_t>(i) + 1, client + "-" + std::to_string(i),
                      "v" + std::to_string(i));
        auto r = driver.ProposeAndWait(AsBytes(cmd), std::chrono::milliseconds(8000));
        if (!r.IsOk() || !r->status.IsOk()) {
          if (failures.fetch_add(1) == 0) {
            const std::lock_guard<std::mutex> lock(first_failure_mutex);
            first_failure = r.IsOk() ? r->status.ToString() : r.GetStatus().ToString();
          }
        }
      }
    });
  }
  for (auto& w : writers) {
    w.join();
  }
  ASSERT_EQ(failures.load(), 0) << "writes failed; the first was: " << first_failure;

  // Everything acknowledged is at or below the leader's commit index, so that is
  // the bar every follower must reach before its state is compared. (This used
  // to be the leader's applied index alone, which swept backwards while the
  // driver re-delivered history, so a follower that was merely behind passed
  // the gate and then looked like it had lost a write.)
  const uint64_t target = std::max(driver.AppliedRaftIndex(), driver.CommitIndex());
  const uint64_t leader_commit_at_target = driver.CommitIndex();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(8000);
  std::string timeline = "target=" + std::to_string(target) +
                         " leader_commit=" + std::to_string(leader_commit_at_target);
  for (auto& [id, node] : nodes_) {
    while (node.driver->AppliedRaftIndex() < target &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    timeline += " | node" + std::to_string(id) +
                " applied_when_wait_ended=" + std::to_string(node.driver->AppliedRaftIndex());
    ASSERT_GE(node.driver->AppliedRaftIndex(), target) << "node " << id << " never caught up";
  }

  for (auto& [id, node] : nodes_) {
    EXPECT_EQ(node.driver->RedeliveredEntries(), 0U)
        << "node " << id << " was handed committed entries more than once";
  }

  for (auto& [id, node] : nodes_) {
    for (int t = 0; t < kThreads; ++t) {
      const std::string client = "writer" + std::to_string(t);
      for (int i = 0; i < kPerThread; ++i) {
        auto got = node.raw_sm->Get(client + "-" + std::to_string(i));
        ASSERT_TRUE(got.IsOk());
        if (!got->has_value()) {
          std::string state;
          for (auto& [nid, n] : nodes_) {
            state += "\n  node " + std::to_string(nid) +
                     " role=" + std::to_string(static_cast<int>(n.driver->CurrentRole())) +
                     " term=" + std::to_string(n.driver->Term()) +
                     " commit=" + std::to_string(n.driver->CommitIndex()) +
                     " raft_applied=" + std::to_string(n.driver->AppliedRaftIndex()) +
                     " sm_applied=" + std::to_string(n.raw_sm->LastAppliedIndex());
          }
          auto session = node.raw_sm->LookupSession(client);
          state += "\n  session(" + client + ") on node " + std::to_string(id) + ": " +
                   (session.IsOk() && session->has_value()
                        ? "last_seq=" + std::to_string((*session)->last_seq()) +
                              " last_touch=" + std::to_string((*session)->last_touch_index())
                        : std::string("absent"));
          ADD_FAILURE() << "node " << id << " lost " << client << "-" << i << "\n  " << timeline
                        << state;
          return;
        }
        ASSERT_TRUE(got->has_value()) << "node " << id << " lost " << client << "-" << i;
        EXPECT_EQ(**got, "v" + std::to_string(i));
      }
    }
  }
}

// Acknowledged means durable on a quorum, so it survives the leader dying.
// The core must hand each committed entry to the apply step exactly once. If it
// re-emitted history on every drain, the state machine's index-based dedup would
// hide it -- data would stay correct -- while each commit cost O(log length) and
// the driver's applied index swept 1..N over and over. Counting redeliveries
// makes that visible instead of absorbed.
TEST_F(RaftGrpcClusterTest, EveryCommittedEntryReachesTheApplyStepExactlyOnce) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  for (int i = 0; i < 40; ++i) {
    const std::string cmd = EncodePut("c1", static_cast<uint64_t>(i) + 1, "k" + std::to_string(i),
                                      "v" + std::to_string(i));
    ASSERT_TRUE(nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), kWait).IsOk());
  }
  const uint64_t target = nodes_.at(leader).driver->AppliedRaftIndex();
  WaitForAppliedAtLeast(target, kWait);

  for (auto& [id, node] : nodes_) {
    EXPECT_EQ(node.driver->RedeliveredEntries(), 0U)
        << "node " << id << " was handed committed entries more than once";
  }
}

TEST_F(RaftGrpcClusterTest, AcknowledgedWritesSurviveLosingTheLeader) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  for (int i = 0; i < 10; ++i) {
    const std::string cmd = EncodePut("c1", static_cast<uint64_t>(i) + 1, "k" + std::to_string(i),
                                      "v" + std::to_string(i));
    auto r = nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), kWait);
    ASSERT_TRUE(r.IsOk()) << r.GetStatus().ToString();
  }

  CrashNode(leader);
  const uint64_t successor = WaitForLeader(std::chrono::milliseconds(6000));
  ASSERT_NE(successor, 0U);
  ASSERT_NE(successor, leader);

  // The successor's no-op has to commit before it may serve a read.
  Result<std::optional<std::string>> got = Status::Unavailable("not tried");
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(6000);
  do {
    got = nodes_.at(successor).driver->LinearizableGet("k9", kWait);
    if (got.IsOk()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
  ASSERT_TRUE(got->has_value()) << "an acknowledged write was lost with the old leader";
  EXPECT_EQ(**got, "v9");
}

}  // namespace
}  // namespace raftkv::server
