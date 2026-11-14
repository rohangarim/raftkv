// Measures raftkv against real clusters: real gRPC on 127.0.0.1, real disk, a
// real fsync on every Raft log append. Everything runs in this one process, so
// the numbers describe the software on this machine, not a deployment: there is
// no network latency between nodes and clients compete with servers for the
// same cores.
//
//   raftkv_bench --workloads put,batch,get,scan,txn --threads 1,4,16 \
//                --seconds 4 --repeat 3
//
// Each measurement is repeated and reported as median with min and max, because
// a single run on a shared machine says little.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>

#include "client/kv_client.h"
#include "client/sharded_client.h"
#include "common/log.h"
#include "server/raft_driver.h"
#include "shard/shard_map.h"
#include "statemachine/kv_state_machine.h"
#include "transport/kv_service.h"
#include "transport/raft_service.h"

namespace {

using Clock = std::chrono::steady_clock;
namespace rk = raftkv;

struct Node {
  std::shared_ptr<rk::statemachine::KvStateMachine> sm;
  std::shared_ptr<rk::transport::RaftGrpcService> service;
  std::shared_ptr<rk::transport::KvGrpcService> kv_service;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<rk::server::RaftDriver> driver;
  std::string address;
};

// Three nodes, one Raft group.
class Cluster {
 public:
  Cluster(uint64_t first_id, const std::filesystem::path& root) {
    for (uint64_t i = 0; i < 3; ++i) ids_.push_back(first_id + i);
    for (uint64_t id : ids_) {
      Node n;
      n.service = std::make_shared<rk::transport::RaftGrpcService>();
      n.kv_service = std::make_shared<rk::transport::KvGrpcService>();
      int port = 0;
      grpc::ServerBuilder builder;
      builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
      builder.SetMaxReceiveMessageSize(-1);
      builder.RegisterService(n.service.get());
      builder.RegisterService(n.kv_service.get());
      n.server = builder.BuildAndStart();
      if (!n.server) {
        throw std::runtime_error("could not bind a port");
      }
      n.address = "127.0.0.1:" + std::to_string(port);
      nodes_[id] = std::move(n);
    }
    std::unordered_map<uint64_t, std::string> addresses;
    for (uint64_t id : ids_) addresses[id] = nodes_[id].address;
    for (uint64_t id : ids_) {
      Node& n = nodes_[id];
      const auto dir = root / ("node" + std::to_string(id));
      std::filesystem::create_directories(dir);
      rk::lsm::Options lo;
      lo.dir = dir / "state";
      auto opened = rk::statemachine::KvStateMachine::Open(std::move(lo));
      if (!opened.IsOk()) {
        throw std::runtime_error("state machine: " + opened.GetStatus().ToString());
      }
      n.sm = std::shared_ptr<rk::statemachine::KvStateMachine>(opened.TakeValue().release());
      rk::server::DriverOptions o;
      o.raft_config.id = id;
      o.raft_config.peers = ids_;
      o.raft_config.election_tick = 10;
      o.raft_config.heartbeat_tick = 1;
      o.raft_config.random_seed = id;
      o.storage_dir = dir / "raft";
      o.peer_addresses = addresses;
      o.tick_interval_ms = 15;
      auto driver = rk::server::RaftDriver::Open(std::move(o), n.sm);
      if (!driver.IsOk()) {
        throw std::runtime_error("driver: " + driver.GetStatus().ToString());
      }
      n.driver = std::shared_ptr<rk::server::RaftDriver>(driver.TakeValue().release());
      n.service->SetDriver(n.driver);
      n.kv_service->SetDriver(n.driver);
      n.driver->Start();
    }
    // Wait for a leader.
    const auto deadline = Clock::now() + std::chrono::seconds(10);
    while (Clock::now() < deadline) {
      int leaders = 0;
      for (uint64_t id : ids_) {
        if (nodes_[id].driver->CurrentRole() == rk::raft::Role::kLeader) ++leaders;
      }
      if (leaders == 1) return;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    throw std::runtime_error("no leader elected");
  }

  ~Cluster() {
    for (auto& [id, n] : nodes_) n.driver->Stop();
    for (auto& [id, n] : nodes_) {
      n.server->Shutdown();
      n.server->Wait();
    }
  }

  std::vector<std::string> Addresses() const {
    std::vector<std::string> out;
    // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
    for (uint64_t id : ids_) out.push_back(nodes_.at(id).address);
    return out;
  }

 private:
  std::vector<uint64_t> ids_;
  std::map<uint64_t, Node> nodes_;
};

std::mutex g_error_mutex;
std::string g_first_error;
void NoteError(const std::string& what) {
  std::lock_guard<std::mutex> lock(g_error_mutex);
  if (g_first_error.empty()) g_first_error = what;
}

struct Stats {
  double ops_per_sec = 0;
  double p50_ms = 0, p95_ms = 0, p99_ms = 0;
  uint64_t ops = 0;
  uint64_t errors = 0;
  uint64_t retries = 0;
};

double Percentile(std::vector<uint32_t>& micros, double p) {
  if (micros.empty()) return 0;
  const size_t idx = std::min(micros.size() - 1, static_cast<size_t>(p * static_cast<double>(micros.size())));
  std::nth_element(micros.begin(), micros.begin() + static_cast<std::ptrdiff_t>(idx), micros.end());
  return static_cast<double>(micros[idx]) / 1000.0;
}

// Runs `body(thread_index, stop_flag, latencies, ops, errors, retries)` on N
// threads for `seconds`, after a warm-up, and folds the results.
template <typename Body>
Stats RunThreads(int threads, int seconds, Body body) {
  std::atomic<bool> stop{false};
  std::atomic<bool> measuring{false};
  std::vector<std::vector<uint32_t>> lat(static_cast<size_t>(threads));
  std::vector<uint64_t> ops(static_cast<size_t>(threads), 0), errs(static_cast<size_t>(threads), 0),
      retr(static_cast<size_t>(threads), 0);
  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(threads));
  for (int t = 0; t < threads; ++t) {
    pool.emplace_back([&, t] {
      body(t, stop, measuring, lat[static_cast<size_t>(t)], ops[static_cast<size_t>(t)],
           errs[static_cast<size_t>(t)], retr[static_cast<size_t>(t)]);
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));  // warm-up
  measuring = true;
  const auto start = Clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  const double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
  stop = true;
  for (auto& th : pool) th.join();

  Stats s;
  std::vector<uint32_t> all;
  for (int t = 0; t < threads; ++t) {
    s.ops += ops[static_cast<size_t>(t)];
    s.errors += errs[static_cast<size_t>(t)];
    s.retries += retr[static_cast<size_t>(t)];
    all.insert(all.end(), lat[static_cast<size_t>(t)].begin(), lat[static_cast<size_t>(t)].end());
  }
  s.ops_per_sec = static_cast<double>(s.ops) / elapsed;
  s.p50_ms = Percentile(all, 0.50);
  s.p95_ms = Percentile(all, 0.95);
  s.p99_ms = Percentile(all, 0.99);
  return s;
}

uint32_t Micros(Clock::time_point a) {
  return static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - a).count());
}

rk::client::ClientOptions Opts(const std::vector<std::string>& addrs, const std::string& id) {
  rk::client::ClientOptions o;
  o.addresses = addrs;
  o.client_id = id;
  o.jitter_seed = 1;
  return o;
}

// A client id must be unique for the life of a client: the cluster remembers the
// last sequence number per id and rejects a restarted counter. Every client
// instance in this tool therefore takes a fresh id.
std::string FreshId(const std::string& prefix) {
  static std::atomic<uint64_t> counter{0};
  return prefix + "-" + std::to_string(::getpid()) + "-" + std::to_string(counter.fetch_add(1));
}

std::string Value(size_t n) { return std::string(n, 'v'); }

std::string KeyN(const char* prefix, uint64_t n) {
  char buf[48];
  std::snprintf(buf, sizeof(buf), "%s%012llu", prefix, static_cast<unsigned long long>(n));
  return buf;
}

struct Config {
  std::vector<std::string> workloads{"fsync", "put", "batch", "get", "scan", "txn"};
  std::vector<int> threads{1, 4, 16};
  int seconds = 4;
  int repeat = 3;
  size_t value_size = 100;
  int batch_size = 100;
};

void Report(const std::string& name, int threads, const std::vector<Stats>& runs, const char* unit,
            double unit_per_op = 1.0) {
  std::vector<double> rates;
  // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
  for (const auto& r : runs) rates.push_back(r.ops_per_sec * unit_per_op);
  std::vector<double> sorted = rates;
  std::sort(sorted.begin(), sorted.end());
  const double median = sorted[sorted.size() / 2];
  const double spread = median > 0 ? (sorted.back() - sorted.front()) / median * 100.0 : 0;
  // Latency of the median run by throughput.
  size_t mid = 0;
  for (size_t i = 0; i < runs.size(); ++i) {
    if (rates[i] == median) mid = i;
  }
  uint64_t errors = 0, retries = 0;
  for (const auto& r : runs) {
    errors += r.errors;
    retries += r.retries;
  }
  std::printf("%-22s %3d thr  %10.0f %-8s (min %.0f, max %.0f, spread %4.1f%%%s)  p50 %7.2f  p95 %7.2f  p99 %7.2f ms",
              name.c_str(), threads, median, unit, sorted.front(), sorted.back(), spread,
              spread > 15 ? " FLAG" : "", runs[mid].p50_ms, runs[mid].p95_ms, runs[mid].p99_ms);
  if (errors != 0) {
    std::printf("  errors=%llu", static_cast<unsigned long long>(errors));
    std::lock_guard<std::mutex> lock(g_error_mutex);
    std::printf(" [first: %.120s]", g_first_error.c_str());
    g_first_error.clear();
  }
  if (retries != 0) std::printf("  conflict-retries=%llu", static_cast<unsigned long long>(retries));
  std::printf("\n");
  std::fflush(stdout);
}

}  // namespace

int RunBench(int argc, char** argv) {
  rk::SetLogLevel(rk::LogLevel::kError);
  Config cfg;
  auto split = [](const std::string& s) {
    std::vector<std::string> out;
    size_t i = 0;
    while (i <= s.size()) {
      const size_t j = s.find(',', i);
      out.push_back(s.substr(i, j == std::string::npos ? std::string::npos : j - i));
      if (j == std::string::npos) break;
      i = j + 1;
    }
    return out;
  };
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&] { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
    if (a == "--workloads") cfg.workloads = split(next());
    else if (a == "--threads") {
      cfg.threads.clear();
      for (const auto& t : split(next())) cfg.threads.push_back(std::atoi(t.c_str()));
    } else if (a == "--seconds") cfg.seconds = std::atoi(next().c_str());
    else if (a == "--repeat") cfg.repeat = std::atoi(next().c_str());
    else if (a == "--value-size") cfg.value_size = static_cast<size_t>(std::atoi(next().c_str()));
    else if (a == "--batch-size") cfg.batch_size = std::atoi(next().c_str());
    else {
      std::fprintf(stderr, "usage: raftkv_bench [--workloads fsync,put,batch,get,scan,txn] [--threads 1,4,16] "
                           "[--seconds N] [--repeat N] [--value-size N] [--batch-size N]\n");
      return 2;
    }
  }

  const auto root = std::filesystem::temp_directory_path() / ("raftkv_bench_" + std::to_string(::getpid()));
  std::filesystem::create_directories(root);
  auto wants = [&](const char* w) {
    return std::find(cfg.workloads.begin(), cfg.workloads.end(), w) != cfg.workloads.end();
  };

  std::printf("# raftkv benchmark\n");
  std::printf("# 3-node Raft group(s) on 127.0.0.1, one process, real gRPC, real disk, fsync on every Raft log append\n");
  std::printf("# value=%zu B, %d s per measurement (+1 s warm-up), %d repeats, batch=%d keys\n\n",
              cfg.value_size, cfg.seconds, cfg.repeat, cfg.batch_size);

  if (wants("fsync")) {
    // The floor under every durable write: append 128 bytes and force them to
    // the drive, with the same call the Raft log uses (F_FULLFSYNC on macOS,
    // fdatasync elsewhere). A replicated write pays this on the leader and then
    // again on a follower before it can be acknowledged.
    const auto path = root / "fsync_probe";
    const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    std::vector<uint32_t> micros;
    const std::string record(128, 'x');
    const auto begin = Clock::now();
    for (int i = 0; i < 300; ++i) {
      const auto t0 = Clock::now();
      (void)!::write(fd, record.data(), record.size());
#if defined(__APPLE__)
      ::fcntl(fd, F_FULLFSYNC, 0);
#else
      ::fdatasync(fd);
#endif
      micros.push_back(Micros(t0));
    }
    const double total = std::chrono::duration<double>(Clock::now() - begin).count();
    ::close(fd);
    std::printf("%-22s %3d thr  %10.0f %-8s  p50 %7.2f  p95 %7.2f  p99 %7.2f ms\n\n",
                "append + fsync (raw)", 1, 300.0 / total, "ops/s", Percentile(micros, 0.50),
                Percentile(micros, 0.95), Percentile(micros, 0.99));
  }

  {
    Cluster cluster(1, root / "a");
    const auto addrs = cluster.Addresses();

    if (wants("put")) {
      for (int threads : cfg.threads) {
        std::vector<Stats> runs;
        for (int r = 0; r < cfg.repeat; ++r) {
          // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
          runs.push_back(RunThreads(threads, cfg.seconds, [&](int t, std::atomic<bool>& stop,
                                                               std::atomic<bool>& measuring,
                                                               std::vector<uint32_t>& lat, uint64_t& ops,
                                                               uint64_t& errs, uint64_t&) {
            rk::client::KvClient c(Opts(addrs, FreshId("put")));
            const std::string v = Value(cfg.value_size);
            uint64_t n = 0;
            while (!stop.load()) {
              const auto t0 = Clock::now();
              const auto s = c.Put(KeyN(("p" + std::to_string(r) + "-" + std::to_string(t) + "-").c_str(), n++), v);
              if (measuring.load()) {
                if (s.IsOk()) {
                  lat.push_back(Micros(t0));
                  ++ops;
                } else {
                  ++errs;
                  NoteError("put: " + s.ToString());
                }
              }
            }
          }));
        }
        Report("put (1 key/request)", threads, runs, "ops/s");
      }
    }

    if (wants("batch")) {
      for (int threads : {1, 4}) {
        std::vector<Stats> runs;
        for (int r = 0; r < cfg.repeat; ++r) {
          // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
          runs.push_back(RunThreads(threads, cfg.seconds, [&](int t, std::atomic<bool>& stop,
                                                               std::atomic<bool>& measuring,
                                                               std::vector<uint32_t>& lat, uint64_t& ops,
                                                               uint64_t& errs, uint64_t&) {
            rk::client::KvClient c(Opts(addrs, FreshId("batch")));
            const std::string v = Value(cfg.value_size);
            uint64_t n = 0;
            while (!stop.load()) {
              std::vector<rk::client::BatchWrite> writes;
              writes.reserve(static_cast<size_t>(cfg.batch_size));
              for (int i = 0; i < cfg.batch_size; ++i) {
                writes.push_back({KeyN(("b" + std::to_string(r) + "-" + std::to_string(t) + "-").c_str(), n++), v});
              }
              const auto t0 = Clock::now();
              auto o = c.Batch({}, writes);
              if (measuring.load()) {
                if (o.IsOk() && o->applied) {
                  lat.push_back(Micros(t0));
                  ++ops;
                } else {
                  ++errs;
                  NoteError(o.IsOk() ? "batch: not applied" : "batch: " + o.GetStatus().ToString());
                }
              }
            }
          }));
        }
        Report("batch write", threads, runs, "keys/s", static_cast<double>(cfg.batch_size));
      }
    }

    if (wants("get") || wants("scan")) {
      // Load a dataset once, untimed.
      constexpr uint64_t kKeys = 20000;
      rk::client::KvClient loader(Opts(addrs, FreshId("loader")));
      for (uint64_t base = 0; base < kKeys; base += 500) {
        std::vector<rk::client::BatchWrite> w;
        for (uint64_t i = base; i < base + 500; ++i) w.push_back({KeyN("d", i), Value(cfg.value_size)});
        if (!loader.Batch({}, w).IsOk()) {
          std::fprintf(stderr, "load failed\n");
          return 1;
        }
      }
      if (wants("get")) {
        for (int threads : cfg.threads) {
          std::vector<Stats> runs;
          for (int r = 0; r < cfg.repeat; ++r) {
            // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
            runs.push_back(RunThreads(threads, cfg.seconds, [&](int t, std::atomic<bool>& stop,
                                                                 std::atomic<bool>& measuring,
                                                                 std::vector<uint32_t>& lat, uint64_t& ops,
                                                                 uint64_t& errs, uint64_t&) {
              rk::client::KvClient c(Opts(addrs, FreshId("get")));
              std::mt19937_64 rng(static_cast<uint64_t>(t) + 7);
              while (!stop.load()) {
                const auto t0 = Clock::now();
                auto g = c.Get(KeyN("d", rng() % kKeys));
                if (measuring.load()) {
                  if (g.IsOk()) {
                    lat.push_back(Micros(t0));
                    ++ops;
                  } else {
                    ++errs;
                  }
                }
              }
            }));
          }
          Report("get (linearizable)", threads, runs, "ops/s");
        }
      }
      if (wants("scan")) {
        for (int threads : {1, 4}) {
          std::vector<Stats> runs;
          for (int r = 0; r < cfg.repeat; ++r) {
            // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
            runs.push_back(RunThreads(threads, cfg.seconds, [&](int t, std::atomic<bool>& stop,
                                                                 std::atomic<bool>& measuring,
                                                                 std::vector<uint32_t>& lat, uint64_t& ops,
                                                                 uint64_t& errs, uint64_t&) {
              rk::client::KvClient c(Opts(addrs, FreshId("scan")));
              std::mt19937_64 rng(static_cast<uint64_t>(t) + 11);
              while (!stop.load()) {
                const auto t0 = Clock::now();
                auto page = c.Scan(KeyN("d", rng() % (kKeys - 200)), "", 100);
                if (measuring.load()) {
                  if (page.IsOk()) {
                    lat.push_back(Micros(t0));
                    ++ops;
                  } else {
                    ++errs;
                  }
                }
              }
            }));
          }
          Report("scan 100 rows", threads, runs, "rows/s", 100.0);
        }
      }
    }
  }

  if (wants("txn")) {
    // Two shards. Accounts "acct-a<i>" live on shard 1, "acct-z<i>" on shard 2.
    Cluster c1(1, root / "s1");
    Cluster c2(4, root / "s2");
    auto map = rk::shard::ShardMap::Create(
        {{1, "", c1.Addresses()}, {2, "acct-m", c2.Addresses()}});
    if (!map.IsOk()) return 1;
    constexpr int kAccounts = 500;
    {
      rk::client::ShardedClient setup(*map, Opts({}, FreshId("txn-setup")));
      for (int i = 0; i < kAccounts; ++i) {
        (void)setup.Put("acct-a" + std::to_string(i), "1000");
        (void)setup.Put("acct-z" + std::to_string(i), "1000");
      }
    }
    for (bool cross : {false, true}) {
      for (int threads : {1, 4, 8}) {
        std::vector<Stats> runs;
        for (int r = 0; r < cfg.repeat; ++r) {
          // NOLINTNEXTLINE(performance-inefficient-vector-operation): benchmark setup, not a hot path
          runs.push_back(RunThreads(threads, cfg.seconds, [&](int t, std::atomic<bool>& stop,
                                                               std::atomic<bool>& measuring,
                                                               std::vector<uint32_t>& lat, uint64_t& ops,
                                                               uint64_t& errs, uint64_t& retries) {
            rk::client::ShardedClient client(
                *map, Opts({}, FreshId("txn")));
            std::mt19937_64 rng(static_cast<uint64_t>(t) * 31 + 5);
            while (!stop.load()) {
              const int i = static_cast<int>(rng() % kAccounts);
              int j = static_cast<int>(rng() % kAccounts);
              // Same-shard transfer: both on shard 1. Cross-shard: one on each.
              const std::string from = "acct-a" + std::to_string(i);
              std::string to = (cross ? "acct-z" : "acct-a") + std::to_string(j);
              if (to == from) continue;
              const auto t0 = Clock::now();
              bool done = false;
              for (int attempt = 0; attempt < 20 && !done; ++attempt) {
                auto txn = client.Begin();
                auto f = txn.Get(from);
                auto g = txn.Get(to);
                if (!f.IsOk() || !g.IsOk()) break;
                txn.Put(from, std::to_string(std::stoi(**f) - 1));
                txn.Put(to, std::to_string(std::stoi(**g) + 1));
                auto ok = txn.Commit();
                if (!ok.IsOk()) break;
                if (*ok) done = true;
                else if (measuring.load()) ++retries;
              }
              if (measuring.load()) {
                if (done) {
                  lat.push_back(Micros(t0));
                  ++ops;
                } else {
                  ++errs;
                  NoteError("transfer: gave up after retries or an error");
                }
              }
            }
          }));
        }
        Report(cross ? "transfer, cross-shard" : "transfer, one shard", threads, runs, "txn/s");
      }
    }
  }

  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  return 0;
}

int main(int argc, char** argv) {
  try {
    return RunBench(argc, argv);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "raftkv_bench: %s\n", e.what());
    return 1;
  }
}
