#pragma once

// Shared fixture: a real Raft cluster on 127.0.0.1 with real gRPC servers, real
// disk, and real KvStateMachines, serving both the Raft RPCs and the
// client-facing Kv service from one port per node. Used by every test that
// needs a live cluster, so none of them re-derives how to bring one up, crash
// a node, or restart it against the same directory.

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "kv.pb.h"
#include "raft.grpc.pb.h"

#include "server/raft_driver.h"
#include "statemachine/kv_state_machine.h"
#include "transport/kv_service.h"
#include "transport/raft_service.h"

namespace raftkv::server::testing {

inline int NextTempId() {
  static int counter = 0;
  return counter++;
}

inline std::string EncodePut(const std::string& client, uint64_t seq, const std::string& key,
                             const std::string& value) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  c.mutable_put()->set_key(key);
  c.mutable_put()->set_value(value);
  return c.SerializeAsString();
}

inline std::span<const std::byte> AsBytes(const std::string& s) {
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
  std::shared_ptr<transport::KvGrpcService> kv_service;
  std::unique_ptr<grpc::Server> grpc_server;
  std::shared_ptr<RaftDriver> driver;
};

class GrpcClusterFixture : public ::testing::Test {
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
      node.kv_service = std::make_shared<transport::KvGrpcService>();

      int bound_port = 0;
      grpc::ServerBuilder builder;
      builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &bound_port);
      builder.RegisterService(node.service.get());
      builder.RegisterService(node.kv_service.get());
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
      options.raft_config.election_tick = election_tick_;
      options.raft_config.heartbeat_tick = 1;
      options.raft_config.random_seed = id;
      options.storage_dir = node.dir / "raft";
      options.peer_addresses = addresses;
      options.tick_interval_ms = 15;

      auto driver_opened = RaftDriver::Open(std::move(options), node.sm);
      ASSERT_TRUE(driver_opened.IsOk()) << driver_opened.GetStatus().ToString();
      node.driver = std::shared_ptr<RaftDriver>(driver_opened.TakeValue().release());
      node.service->SetDriver(node.driver);
      node.kv_service->SetDriver(node.driver);
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
    node.kv_service.reset();
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
    node.kv_service = std::make_shared<transport::KvGrpcService>();

    int bound_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &bound_port);
    builder.RegisterService(node.service.get());
    builder.RegisterService(node.kv_service.get());
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
    options.raft_config.election_tick = election_tick_;
    options.raft_config.heartbeat_tick = 1;
    options.raft_config.random_seed = id;
    options.storage_dir = node.dir / "raft";  // SAME directory: this is the recovery test
    options.peer_addresses = addresses;
    options.tick_interval_ms = 15;

    auto driver_opened = RaftDriver::Open(std::move(options), node.sm);
    ASSERT_TRUE(driver_opened.IsOk()) << driver_opened.GetStatus().ToString();
    node.driver = std::shared_ptr<RaftDriver>(driver_opened.TakeValue().release());
    node.service->SetDriver(node.driver);
    node.kv_service->SetDriver(node.driver);
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

  // Ticks before a follower starts an election. The driver holds its global
  // lock across every fsync, so under heavy concurrent writes the ticker is
  // starved and heartbeats arrive late; a heavily loaded test raises this so it
  // measures replication ordering rather than lock contention. Group commit
  // (Phase 9) is what removes the underlying cause.
  uint32_t election_tick_ = 10;

  std::filesystem::path root_;
  std::unordered_map<uint64_t, RealNode> nodes_;
};

}  // namespace raftkv::server::testing
