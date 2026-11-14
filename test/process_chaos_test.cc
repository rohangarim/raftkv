// The same linearizability campaign as the in-process test, but against three
// real raftkv_node processes. What this adds over that test:
//   * nodes die by SIGKILL, so a write can be cut off mid-fsync with nothing
//     flushed and no chance to shut down cleanly;
//   * the network can partition: every node-to-node link runs through a relay
//     (link_proxy.h) that the test can cut, isolating one node (the leader half
//     the time) while it is still alive and still believes what it believed.
//
// Clients talk to the nodes directly. After the chaos stops and the cluster has
// healed, the test reads every key once more; those reads join the history, so
// the final state has to be explainable too.

#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/kv_client.h"
#include "history_workload.h"
#include "link_proxy.h"
#include "linearizability.h"

extern char** environ;  // NOLINT(readability-identifier-naming)

namespace raftkv::testing {
namespace {

namespace rk = raftkv::client;
namespace fs = std::filesystem;

uint16_t FreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  (void)::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  const uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

class ProcessChaosTest : public ::testing::Test {
 protected:
  static constexpr int kNodes = 3;

  void SetUp() override {
    root_ = fs::temp_directory_path() / ("raftkv-chaos-" + std::to_string(::getpid()));
    fs::remove_all(root_);
    fs::create_directories(root_);
    for (int i = 1; i <= kNodes; ++i) real_port_[i] = FreePort();
    // One relay per ordered pair: node i reaches node j through proxy_[i][j].
    for (int i = 1; i <= kNodes; ++i) {
      for (int j = 1; j <= kNodes; ++j) {
        if (i != j) proxy_[i][j] = std::make_unique<LinkProxy>(real_port_[j]);
      }
    }
  }

  void TearDown() override {
    for (int i = 1; i <= kNodes; ++i) Kill(i);
    proxy_.clear();
    if (std::getenv("RAFTKV_KEEP_CHAOS_DIR") == nullptr) fs::remove_all(root_);  // NOLINT(concurrency-mt-unsafe)
  }

  std::string PeersFor(int self) const {
    std::string spec;
    for (int j = 1; j <= kNodes; ++j) {
      if (!spec.empty()) spec += ",";
      const uint16_t port = (j == self) ? real_port_.at(j) : proxy_.at(self).at(j)->port();
      spec += std::to_string(j) + "=127.0.0.1:" + std::to_string(port);
    }
    return spec;
  }

  void Spawn(int id) {
    const fs::path dir = root_ / ("node" + std::to_string(id));
    fs::create_directories(dir);
    const std::string id_s = std::to_string(id);
    const std::string dir_s = (dir / "data").string();
    const std::string peers = PeersFor(id);
    std::vector<const char*> argv{RAFTKV_NODE_BIN, "--id", id_s.c_str(), "--dir", dir_s.c_str(),
                                  "--peers", peers.c_str(), nullptr};

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    const std::string log = (root_ / ("node" + std::to_string(id) + ".log")).string();
    posix_spawn_file_actions_addopen(&actions, 1, log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    posix_spawn_file_actions_adddup2(&actions, 1, 2);
    pid_t pid = 0;
    const int rc = posix_spawn(&pid, RAFTKV_NODE_BIN, &actions, nullptr,
                               const_cast<char* const*>(argv.data()), environ);
    posix_spawn_file_actions_destroy(&actions);
    ASSERT_EQ(rc, 0) << "could not start " << RAFTKV_NODE_BIN;
    pid_[id] = pid;
  }

  void Kill(int id) {
    if (pid_[id] > 0) {
      ::kill(pid_[id], SIGKILL);
      int status = 0;
      ::waitpid(pid_[id], &status, 0);
      pid_[id] = 0;
    }
  }

  void SetIsolated(int id, bool cut) {
    for (int j = 1; j <= kNodes; ++j) {
      if (j == id) continue;
      proxy_[id][j]->SetCut(cut);
      proxy_[j][id]->SetCut(cut);
    }
  }

  std::vector<std::string> RealAddresses() const {
    std::vector<std::string> out;
    for (int i = 1; i <= kNodes; ++i) out.push_back("127.0.0.1:" + std::to_string(real_port_.at(i)));
    return out;
  }

  rk::ClientOptions Options(uint64_t seed) {
    rk::ClientOptions o;
    o.addresses = RealAddresses();
    o.client_id = "chaos-" + std::to_string(id_counter_.fetch_add(1));
    o.attempt_timeout = std::chrono::milliseconds(700);
    o.total_timeout = std::chrono::milliseconds(2500);
    o.jitter_seed = seed;
    return o;
  }

  // Which node the clients currently believe leads, or 0.
  int LeaderGuess() {
    rk::KvClient probe(Options(1));
    auto r = probe.Get("probe");
    if (!r.IsOk()) return 0;
    const std::string leader = probe.CachedLeader();
    for (int i = 1; i <= kNodes; ++i) {
      if (leader == "127.0.0.1:" + std::to_string(real_port_.at(i))) return i;
    }
    return 0;
  }

  fs::path root_;
  std::map<int, uint16_t> real_port_;
  std::map<int, std::map<int, std::unique_ptr<LinkProxy>>> proxy_;
  std::map<int, pid_t> pid_;
  std::atomic<uint64_t> id_counter_{0};
};

TEST_F(ProcessChaosTest, HistoryUnderSigkillAndPartitionsIsLinearizable) {
  uint64_t seed = 1;
  if (const char* s = std::getenv("RAFTKV_LIN_SEED")) seed = std::strtoull(s, nullptr, 10);  // NOLINT(concurrency-mt-unsafe)

  for (int i = 1; i <= kNodes; ++i) {
    Spawn(i);
    ASSERT_GT(pid_[i], 0);
  }
  // Wait for a working cluster before the clock starts.
  bool up = false;
  for (int attempt = 0; attempt < 100 && !up; ++attempt) {
    rk::KvClient probe(Options(1));
    up = probe.Put("warmup", "1").IsOk();
    if (!up) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(up) << "the cluster never came up; logs in " << root_;

  std::atomic<bool> done{false};
  std::atomic<int> kills{0};
  std::atomic<int> partitions{0};
  std::atomic<int> double_faults{0};

  std::thread chaos([&] {
    std::mt19937_64 rng(seed * 7919);
    int event = 0;
    while (!done.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200 + rng() % 500));
      if (done.load()) break;
      const int victim = 1 + static_cast<int>(rng() % kNodes);
      switch (event++ % 4) {
        case 0: {  // SIGKILL one node, restart it on the same directory.
          Kill(victim);
          std::this_thread::sleep_for(std::chrono::milliseconds(150 + rng() % 600));
          Spawn(victim);
          ++kills;
          break;
        }
        case 1:
        case 3: {  // Cut one node off, the leader if we can tell which it is.
          int target = victim;
          if (event % 2 == 0) {
            const int leader = LeaderGuess();
            if (leader != 0) target = leader;
          }
          SetIsolated(target, true);
          std::this_thread::sleep_for(std::chrono::milliseconds(600 + rng() % 1500));
          SetIsolated(target, false);
          ++partitions;
          break;
        }
        default: {  // Two nodes down at once: no quorum, so outcomes go unknown.
          const int other = 1 + (victim % kNodes);
          Kill(victim);
          Kill(other);
          std::this_thread::sleep_for(std::chrono::milliseconds(2200));
          Spawn(victim);
          Spawn(other);
          ++double_faults;
          break;
        }
      }
    }
  });

  WorkloadConfig wc;
  wc.threads = 5;
  wc.ops_per_thread = 200;
  wc.keys = 5;
  wc.seed = seed;
  std::vector<HistoryOp> history = RunWorkload(wc, [this](uint64_t s) { return Options(s); });
  done.store(true);
  chaos.join();

  // Heal everything and read the final state of every key.
  for (int i = 1; i <= kNodes; ++i) SetIsolated(i, false);
  for (int i = 1; i <= kNodes; ++i) {
    if (pid_[i] <= 0) Spawn(i);
  }
  for (int k = 0; k < wc.keys; ++k) {
    HistoryOp op;
    op.kind = RegOp::kGet;
    op.key = "k" + std::to_string(k);
    op.thread = -1;
    for (int attempt = 0; attempt < 30; ++attempt) {
      rk::KvClient client(Options(1000 + static_cast<uint64_t>(attempt)));
      op.invoke = NowNanos();
      auto r = client.Get(op.key);
      op.ret = NowNanos();
      if (r.IsOk()) {
        op.completed = true;
        op.observed = *r;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(op.completed) << "cluster did not recover after healing; logs in " << root_;
    history.push_back(std::move(op));
  }

  size_t unknown = 0;
  size_t reads = 0;
  size_t swaps = 0;
  for (const auto& op : history) {
    if (!op.completed) ++unknown;
    if (op.kind == RegOp::kGet && op.completed) ++reads;
    if (op.kind == RegOp::kCas && op.completed && op.applied) ++swaps;
  }
  std::printf("process campaign: %zu ops, %zu unknown, %d SIGKILLs, %d partitions, %d double faults, "
              "%zu reads, %zu successful swaps\n",
              history.size(), unknown, kills.load(), partitions.load(), double_faults.load(), reads, swaps);

  EXPECT_GE(kills.load() + partitions.load() + double_faults.load(), 3);
  EXPECT_GT(history.size() - unknown, history.size() / 3) << "cluster was unavailable most of the run";
  const CheckResult r = CheckLinearizable(history);
  EXPECT_FALSE(r.inconclusive) << "checker ran out of budget";
  EXPECT_TRUE(r.linearizable) << DescribeFailure(r);
}

}  // namespace
}  // namespace raftkv::testing
