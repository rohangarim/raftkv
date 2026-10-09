// Standalone node process.
//
// Hosts one Raft group: a RaftDriver over gRPC, backed by a KvStateMachine on
// disk. This is deliberately minimal -- no client-facing RPC yet (that is
// Phase 4), no multi-group hosting (a node process holding several groups is
// noted as a scaling ceiling in the original design and not built here
// either). What it does support is real process lifecycle: start, run, and
// -- the reason it exists at all -- be killed with SIGKILL and come back.
//
// usage:
//   raftkv_node --id 1 --dir /var/lib/raftkv/node1 \
//       --peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "lsm/db.h"

#include "common/log.h"
#include "server/raft_driver.h"
#include "statemachine/kv_state_machine.h"
#include "transport/raft_service.h"

namespace {

std::atomic<bool> g_shutdown{false};

void HandleSignal(int /*signum*/) { g_shutdown.store(true); }

struct ParsedArgs {
  uint64_t id = 0;
  std::filesystem::path dir;
  std::unordered_map<uint64_t, std::string> peers;  // includes self
};

bool ParsePeers(const std::string& spec, std::unordered_map<uint64_t, std::string>* out) {
  std::stringstream ss(spec);
  std::string entry;
  while (std::getline(ss, entry, ',')) {
    const auto eq = entry.find('=');
    if (eq == std::string::npos) {
      return false;
    }
    uint64_t id = 0;
    try {
      id = std::stoull(entry.substr(0, eq));
    } catch (...) {
      return false;
    }
    (*out)[id] = entry.substr(eq + 1);
  }
  return !out->empty();
}

bool Parse(int argc, char** argv, ParsedArgs* out) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string(); };
    if (arg == "--id") {
      out->id = std::strtoull(next().c_str(), nullptr, 10);
    } else if (arg == "--dir") {
      out->dir = next();
    } else if (arg == "--peers") {
      if (!ParsePeers(next(), &out->peers)) {
        std::fprintf(stderr, "invalid --peers spec\n");
        return false;
      }
    } else {
      std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
      return false;
    }
  }
  if (out->id == 0 || out->dir.empty() || out->peers.find(out->id) == out->peers.end()) {
    std::fprintf(stderr,
                 "usage: raftkv_node --id N --dir PATH "
                 "--peers 1=host:port,2=host:port,...\n");
    return false;
  }
  return true;
}

}  // namespace

int RunNode(int argc, char** argv) {
  ParsedArgs args;
  if (!Parse(argc, argv, &args)) {
    return 2;
  }

  std::signal(SIGTERM, HandleSignal);
  std::signal(SIGINT, HandleSignal);

  raftkv::lsm::Options lsm_options;
  lsm_options.dir = args.dir / "state";
  auto sm_opened = raftkv::statemachine::KvStateMachine::Open(std::move(lsm_options));
  if (!sm_opened.IsOk()) {
    std::fprintf(stderr, "failed to open state machine: %s\n",
                 sm_opened.GetStatus().ToString().c_str());
    return 1;
  }
  std::shared_ptr<raftkv::statemachine::StateMachine> state_machine(
      sm_opened.TakeValue().release());

  auto service = std::make_shared<raftkv::transport::RaftGrpcService>();

  const std::string& self_address = args.peers.at(args.id);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(self_address, grpc::InsecureServerCredentials());
  builder.RegisterService(service.get());
  std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
  if (grpc_server == nullptr) {
    std::fprintf(stderr, "failed to bind %s\n", self_address.c_str());
    return 1;
  }

  raftkv::server::DriverOptions driver_options;
  driver_options.raft_config.id = args.id;
  for (auto& [id, addr] : args.peers) {
    driver_options.raft_config.peers.push_back(id);
  }
  driver_options.raft_config.election_tick = 10;
  driver_options.raft_config.heartbeat_tick = 1;
  driver_options.raft_config.random_seed = args.id;
  driver_options.storage_dir = args.dir / "raft";
  driver_options.peer_addresses = args.peers;
  driver_options.tick_interval_ms = 50;

  auto driver_opened = raftkv::server::RaftDriver::Open(std::move(driver_options), state_machine);
  if (!driver_opened.IsOk()) {
    std::fprintf(stderr, "failed to open raft driver: %s\n",
                 driver_opened.GetStatus().ToString().c_str());
    return 1;
  }
  std::shared_ptr<raftkv::server::RaftDriver> driver(driver_opened.TakeValue().release());
  service->SetDriver(driver);
  driver->Start();

  LOG_INFO("node %llu listening on %s", static_cast<unsigned long long>(args.id),
           self_address.c_str());

  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG_INFO("node %llu shutting down", static_cast<unsigned long long>(args.id));
  driver->Stop();
  grpc_server->Shutdown();
  grpc_server->Wait();
  return 0;
}

int main(int argc, char** argv) {
  // An exception escaping main would terminate with no message, and this is
  // the one process whose failures an operator has to diagnose from its log.
  try {
    return RunNode(argc, argv);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
  } catch (...) {
    std::fprintf(stderr, "fatal: unknown exception\n");
  }
  return 1;
}
