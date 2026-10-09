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

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "kv.pb.h"
#include "raft.grpc.pb.h"

#include "server/raft_driver.h"
#include "statemachine/kv_state_machine.h"
#include "transport/raft_service.h"

namespace raftkv::server {
namespace {

int NextTempId() {
  static int counter = 0;
  return counter++;
}

std::string EncodePut(const std::string& client, uint64_t seq, const std::string& key,
                      const std::string& value) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  c.mutable_put()->set_key(key);
  c.mutable_put()->set_value(value);
  return c.SerializeAsString();
}

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// One node's full stack: gRPC server, service, driver, state machine. Owns
// its on-disk directory so the node can be torn down and rebuilt against it
// to simulate a crash and restart.
struct RealNode {
  uint64_t id = 0;
  std::filesystem::path dir;
  std::string address;

  std::shared_ptr<statemachine::KvStateMachine> raw_sm;  // for direct Get() in assertions
  std::shared_ptr<statemachine::StateMachine> sm;
  std::shared_ptr<transport::RaftGrpcService> service;
  std::unique_ptr<grpc::Server> grpc_server;
  std::shared_ptr<RaftDriver> driver;
};

class RaftGrpcClusterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ =
        std::filesystem::temp_directory_path() /
        ("raftkv_grpc_cluster_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(root_);
  }

  void TearDown() override {
    for (auto& [id, node] : nodes_) {
      if (node.driver) {
        node.driver->Stop();
      }
      if (node.grpc_server) {
        node.grpc_server->Shutdown();
        node.grpc_server->Wait();
      }
    }
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  // Phase 1: bind gRPC servers on ephemeral ports for every id, with a
  // driver-less service. Learns every node's real address.
  void BindServers(const std::vector<uint64_t>& ids) {
    for (uint64_t id : ids) {
      RealNode node;
      node.id = id;
      node.dir = root_ / ("node" + std::to_string(id));
      std::filesystem::create_directories(node.dir);
      node.service = std::make_shared<transport::RaftGrpcService>();

      int bound_port = 0;
      grpc::ServerBuilder builder;
      builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &bound_port);
      builder.RegisterService(node.service.get());
      node.grpc_server = builder.BuildAndStart();
      ASSERT_NE(node.grpc_server, nullptr) << "node " << id << " failed to bind";
      node.address = "127.0.0.1:" + std::to_string(bound_port);

      nodes_.emplace(id, std::move(node));
    }
  }

  // Phase 2: now that every address is known, build the real driver for each
  // node and attach it to its already-listening service.
  void StartDrivers(const std::vector<uint64_t>& ids) {
    std::unordered_map<uint64_t, std::string> addresses;
    for (uint64_t id : ids) {
      addresses[id] = nodes_.at(id).address;
    }

    for (uint64_t id : ids) {
      RealNode& node = nodes_.at(id);

      lsm::Options lsm_options;
      lsm_options.dir = node.dir / "state";
      auto sm_opened = statemachine::KvStateMachine::Open(std::move(lsm_options));
      ASSERT_TRUE(sm_opened.IsOk()) << sm_opened.GetStatus().ToString();
      node.raw_sm = std::shared_ptr<statemachine::KvStateMachine>(sm_opened.TakeValue().release());
      node.sm = node.raw_sm;

      DriverOptions options;
      options.raft_config.id = id;
      options.raft_config.peers = ids;
      options.raft_config.election_tick = 10;
      options.raft_config.heartbeat_tick = 1;
      options.raft_config.random_seed = id;
      options.storage_dir = node.dir / "raft";
      options.peer_addresses = addresses;
      options.tick_interval_ms = 15;

      auto driver_opened = RaftDriver::Open(std::move(options), node.sm);
      ASSERT_TRUE(driver_opened.IsOk()) << driver_opened.GetStatus().ToString();
      node.driver = std::shared_ptr<RaftDriver>(driver_opened.TakeValue().release());
      node.service->SetDriver(node.driver);
      node.driver->Start();
    }
  }

  void StartCluster(const std::vector<uint64_t>& ids) {
    BindServers(ids);
    StartDrivers(ids);
  }

  // Crashes a node: stops its driver and shuts down its gRPC server, WITHOUT
  // touching its on-disk directory. The server (and a fresh driver on top of
  // the same directory) is rebuilt by RestartNode.
  void CrashNode(uint64_t id) {
    RealNode& node = nodes_.at(id);
    node.driver->Stop();
    node.grpc_server->Shutdown();
    node.grpc_server->Wait();
    node.driver.reset();
    node.grpc_server.reset();
    node.service.reset();
    node.sm.reset();
    node.raw_sm.reset();
  }

  // Rebinds the SAME address (ephemeral ports are not reused automatically by
  // the OS on this short a timescale in practice for a test's lifetime, but to
  // be correct regardless we rebind fresh and update the peer address map on
  // every other node, exactly as a real operator restarting a node at a new
  // port would have to).
  void RestartNode(uint64_t id, const std::vector<uint64_t>& all_ids) {
    RealNode& node = nodes_.at(id);
    node.service = std::make_shared<transport::RaftGrpcService>();

    int bound_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &bound_port);
    builder.RegisterService(node.service.get());
    node.grpc_server = builder.BuildAndStart();
    ASSERT_NE(node.grpc_server, nullptr);
    node.address = "127.0.0.1:" + std::to_string(bound_port);

    // The address changed, so every peer's dial target must be updated too.
    std::unordered_map<uint64_t, std::string> addresses;
    for (uint64_t peer : all_ids) {
      addresses[peer] = nodes_.at(peer).address;
    }
    for (uint64_t peer : all_ids) {
      if (peer != id) {
        nodes_.at(peer).driver->UpdatePeerAddress(id, node.address);
      }
    }

    lsm::Options lsm_options;
    lsm_options.dir = node.dir / "state";
    auto sm_opened = statemachine::KvStateMachine::Open(std::move(lsm_options));
    ASSERT_TRUE(sm_opened.IsOk()) << sm_opened.GetStatus().ToString();
    node.raw_sm = std::shared_ptr<statemachine::KvStateMachine>(sm_opened.TakeValue().release());
    node.sm = node.raw_sm;

    DriverOptions options;
    options.raft_config.id = id;
    options.raft_config.peers = all_ids;
    options.raft_config.election_tick = 10;
    options.raft_config.heartbeat_tick = 1;
    options.raft_config.random_seed = id;
    options.storage_dir = node.dir / "raft";  // SAME directory: this is the recovery test
    options.peer_addresses = addresses;
    options.tick_interval_ms = 15;

    auto driver_opened = RaftDriver::Open(std::move(options), node.sm);
    ASSERT_TRUE(driver_opened.IsOk()) << driver_opened.GetStatus().ToString();
    node.driver = std::shared_ptr<RaftDriver>(driver_opened.TakeValue().release());
    node.service->SetDriver(node.driver);
    node.driver->Start();
  }

  uint64_t WaitForLeader(std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      uint64_t leader = 0;
      int leader_count = 0;
      for (auto& [id, node] : nodes_) {
        if (node.driver && node.driver->CurrentRole() == raft::Role::kLeader) {
          leader = id;
          ++leader_count;
        }
      }
      if (leader_count == 1) {
        return leader;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return 0;
  }

  // Waits for the RAFT LOG's commit index, not the state machine's applied
  // index. The two are NOT interchangeable: CommitIndex() advances the moment
  // Step() processes an AppendEntries/heartbeat, inside the driver's lock,
  // while ApplyCommitted() runs AFTER that lock is released (deliberately --
  // holding the lock across a state-machine write would serialize every
  // node's apply path behind its own network I/O). A caller that needs
  // applied state, not just committed state, must wait on
  // KvStateMachine::LastAppliedIndex() instead. This is not a technicality:
  // it is exactly the gap Phase 4's ReadIndex protocol has to close for
  // linearizable reads ("confirm leadership, then wait until applied >=
  // readIndex"), and a test that blurs the two will pass by accident far more
  // often than it fails, which is worse than failing consistently.
  void WaitForCommitAtLeast(uint64_t index, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      bool all_there = true;
      for (auto& [id, node] : nodes_) {
        if (!node.driver || node.driver->CommitIndex() < index) {
          all_there = false;
        }
      }
      if (all_there) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void WaitForAppliedAtLeast(uint64_t index, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      bool all_there = true;
      for (auto& [id, node] : nodes_) {
        if (!node.raw_sm || node.raw_sm->LastAppliedIndex() < index) {
          all_there = false;
        }
      }
      if (all_there) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  std::filesystem::path root_;
  std::unordered_map<uint64_t, RealNode> nodes_;
};

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

}  // namespace
}  // namespace raftkv::server
