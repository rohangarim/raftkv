// Storage-engine microbenchmark: the same workloads against each engine behind
// engine::Engine, with no Raft, no network and no fsync on the apply path.
//
// Why this exists alongside raftkv_bench. The replicated write path is bound by
// the Raft log's fsync, so an end-to-end write benchmark cannot tell two engines
// apart on writes (it measures the log). What an engine DOES change is covered
// here: what an apply costs, what reads and scans cost, and above all what a
// consistent snapshot costs the writers around it -- the case the built-in engine
// handles by flushing and compacting everything first (DECISIONS.md D-0024).
//
//   engine_bench --engine builtin|lsmtree [--keys N] [--value-size N] [--seconds N]
//                [--workloads apply,get,scan,snapshot] [--threads N]
//
// Output is one line per measurement, in the same shape as raftkv_bench, meant to
// be saved verbatim under results/ together with the machine and the commit.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "engine/engine.h"

namespace {

// One place that ends the process on a failed benchmark step, so the
// thread-unsafe exit() is acknowledged once.
[[noreturn]] void Die() {
  std::exit(1);  // NOLINT(concurrency-mt-unsafe): single-threaded failure path
}

namespace eg = raftkv::engine;
using Clock = std::chrono::steady_clock;

struct Config {
  eg::Kind engine = eg::DefaultKind();
  int keys = 200000;
  size_t value_size = 100;
  int seconds = 4;
  int threads = 4;
  std::vector<std::string> workloads{"apply", "get", "scan", "snapshot"};
};

std::string Key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "u%010d", i);
  return buf;
}

double Percentile(std::vector<double>* micros, double p) {
  if (micros->empty()) {
    return 0;
  }
  std::sort(micros->begin(), micros->end());
  const auto idx = static_cast<size_t>(p * static_cast<double>(micros->size() - 1));
  return (*micros)[idx] / 1000.0;  // ms
}

double Millis(Clock::duration d) { return std::chrono::duration<double, std::milli>(d).count(); }

// What one Raft apply writes: the user effect, the client's session entry, and
// the applied index, as a single atomic batch.
void FillApplyBatch(eg::Batch* b, int key, uint64_t index, const std::string& value,
                    const std::string& session) {
  b->Clear();
  b->Put(Key(key), value);
  b->Put("sclient-" + std::to_string(key % 64), session);
  b->Put("mapplied_index", std::string(reinterpret_cast<const char*>(&index), sizeof(index)));
}

void Preload(eg::Engine* e, const Config& cfg) {
  const std::string value(cfg.value_size, 'v');
  eg::Batch b;
  for (int i = 0; i < cfg.keys; ++i) {
    b.Put(Key(i), value);
    if (b.Count() == 100) {
      if (!e->Write(b).IsOk()) {
        std::fprintf(stderr, "preload write failed\n");
        Die();
      }
      b.Clear();
    }
  }
  if (!b.Empty() && !e->Write(b).IsOk()) {
    Die();
  }
  // Overwrite a third of the keys, so the data has shadowed versions to skip or
  // collapse -- the shape a long-lived state machine actually has.
  for (int i = 0; i < cfg.keys; i += 3) {
    b.Put(Key(i), value);
    if (b.Count() == 100) {
      (void)e->Write(b);
      b.Clear();
    }
  }
  if (!b.Empty()) {
    (void)e->Write(b);
  }
}

void Print(const char* name, int threads, double rate, const char* unit,
           std::vector<double>* lat_micros) {
  std::printf("%-28s %3d thr  %10.0f %-8s  p50 %7.3f  p95 %7.3f  p99 %7.3f ms\n", name, threads,
              rate, unit, Percentile(lat_micros, 0.50), Percentile(lat_micros, 0.95),
              Percentile(lat_micros, 0.99));
  std::fflush(stdout);
}

void RunApply(eg::Engine* e, const Config& cfg) {
  const std::string value(cfg.value_size, 'a');
  const std::string session(64, 's');
  std::mt19937_64 rng(1);
  std::vector<double> lat;
  eg::Batch b;
  uint64_t index = 1;
  const auto begin = Clock::now();
  const auto stop = begin + std::chrono::seconds(cfg.seconds);
  uint64_t ops = 0;
  while (Clock::now() < stop) {
    FillApplyBatch(&b, static_cast<int>(rng() % static_cast<uint64_t>(cfg.keys)), index++, value,
                   session);
    const auto t0 = Clock::now();
    if (!e->Write(b).IsOk()) {
      std::fprintf(stderr, "apply write failed\n");
      Die();
    }
    lat.push_back(Millis(Clock::now() - t0) * 1000.0);
    ++ops;
  }
  const double secs = std::chrono::duration<double>(Clock::now() - begin).count();
  Print("apply (3-op atomic batch)", 1, static_cast<double>(ops) / secs, "batch/s", &lat);
}

void RunGet(eg::Engine* e, const Config& cfg, int threads) {
  std::atomic<uint64_t> total{0};
  std::vector<std::vector<double>> lats(static_cast<size_t>(threads));
  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(threads));
  const auto begin = Clock::now();
  const auto stop = begin + std::chrono::seconds(cfg.seconds);
  for (int t = 0; t < threads; ++t) {
    pool.emplace_back([&, t] {
      std::mt19937_64 rng(100 + static_cast<uint64_t>(t));
      auto& lat = lats[static_cast<size_t>(t)];
      uint64_t n = 0;
      while (Clock::now() < stop) {
        const auto t0 = Clock::now();
        auto got = e->Get(Key(static_cast<int>(rng() % static_cast<uint64_t>(cfg.keys))));
        if (!got.IsOk()) {
          std::fprintf(stderr, "get failed\n");
          Die();
        }
        if (n % 8 == 0) {
          lat.push_back(Millis(Clock::now() - t0) * 1000.0);
        }
        ++n;
      }
      total += n;
    });
  }
  for (auto& th : pool) {
    th.join();
  }
  std::vector<double> all;
  for (auto& l : lats) {
    all.insert(all.end(), l.begin(), l.end());
  }
  const double secs = std::chrono::duration<double>(Clock::now() - begin).count();
  Print("get (random point read)", threads, static_cast<double>(total.load()) / secs, "ops/s",
        &all);
}

// Two shapes of the same 100-row scan, reported separately because they stress
// different things. A BOUNDED range ([start, start+100 keys)) is what most
// callers issue. An OPEN-ENDED one (`hi` empty, stop after 100 rows) must not cost
// more than the 100 rows, but an engine that materialises every entry from the
// start key onward before applying the limit pays for the rest of the database.
void RunScanShape(eg::Engine* e, const Config& cfg, bool bounded) {
  std::mt19937_64 rng(7);
  std::vector<double> lat;
  uint64_t rows = 0;
  const auto begin = Clock::now();
  const auto stop = begin + std::chrono::seconds(cfg.seconds);
  while (Clock::now() < stop) {
    const int start = static_cast<int>(rng() % static_cast<uint64_t>(cfg.keys - 200));
    const std::string hi = bounded ? Key(start + 100) : std::string();
    const auto t0 = Clock::now();
    const auto s = e->Scan(Key(start), hi, 100, [&rows](std::string_view, std::string_view) {
      ++rows;
      return raftkv::lsm::Status::Ok();
    });
    if (!s.IsOk()) {
      std::fprintf(stderr, "scan failed\n");
      Die();
    }
    lat.push_back(Millis(Clock::now() - t0) * 1000.0);
  }
  const double secs = std::chrono::duration<double>(Clock::now() - begin).count();
  Print(bounded ? "scan 100 rows (bounded)" : "scan 100 rows (open-ended)", 1,
        static_cast<double>(rows) / secs, "rows/s", &lat);
}

void RunScan(eg::Engine* e, const Config& cfg) {
  RunScanShape(e, cfg, /*bounded=*/true);
  RunScanShape(e, cfg, /*bounded=*/false);
}

// The measurement the swap is about. A writer applies continuously; once without
// anything else running, then while a full consistent scan (what a Raft snapshot
// does) runs alongside it. The question is what the scan costs the writer.
void RunSnapshot(eg::Engine* e, const Config& cfg) {
  const std::string value(cfg.value_size, 'w');
  const std::string session(64, 's');

  struct Window {
    std::vector<double> lat_micros;
    uint64_t ops = 0;
    double max_ms = 0;
  };
  std::atomic<bool> stop_writer{false};
  std::atomic<bool> measuring{false};
  Window window;
  std::mutex mu;

  std::thread writer([&] {
    std::mt19937_64 rng(3);
    eg::Batch b;
    uint64_t index = 1'000'000;
    while (!stop_writer.load()) {
      FillApplyBatch(&b, static_cast<int>(rng() % static_cast<uint64_t>(cfg.keys)), index++, value,
                     session);
      const auto t0 = Clock::now();
      if (!e->Write(b).IsOk()) {
        std::fprintf(stderr, "writer failed\n");
        Die();
      }
      const double ms = Millis(Clock::now() - t0);
      if (measuring.load()) {
        const std::lock_guard<std::mutex> lock(mu);
        window.lat_micros.push_back(ms * 1000.0);
        window.max_ms = std::max(window.max_ms, ms);
        ++window.ops;
      }
    }
  });

  auto measure = [&](const char* label, auto&& body) {
    {
      const std::lock_guard<std::mutex> lock(mu);
      window = Window{};
    }
    measuring.store(true);
    const auto t0 = Clock::now();
    body();
    const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
    measuring.store(false);
    const std::lock_guard<std::mutex> lock(mu);
    std::printf("%-28s window %8.3f s  writer %8.0f batch/s  p50 %7.3f  p99 %7.3f  max %8.3f ms\n",
                label, secs, static_cast<double>(window.ops) / secs,
                Percentile(&window.lat_micros, 0.50), Percentile(&window.lat_micros, 0.99),
                window.max_ms);
    std::fflush(stdout);
    return secs;
  };

  std::this_thread::sleep_for(std::chrono::milliseconds(300));  // let the writer reach steady state

  // Baseline writer behaviour over a window the same length as the scan will be.
  // The scan's length is not known in advance, so run it first to find out, then
  // measure the baseline for that long. Two scans, one discarded as warm-up.
  uint64_t rows = 0;
  const auto scan_once = [&] {
    rows = 0;
    const auto s = e->ScanAll([&rows](std::string_view, std::string_view) {
      ++rows;
      return raftkv::lsm::Status::Ok();
    });
    if (!s.IsOk()) {
      std::fprintf(stderr, "snapshot scan failed: %s\n", s.ToString().c_str());
      Die();
    }
  };
  const auto warm0 = Clock::now();
  scan_once();
  const double warm_secs = std::chrono::duration<double>(Clock::now() - warm0).count();

  measure("writer alone", [&] {
    std::this_thread::sleep_for(std::chrono::duration<double>(std::max(warm_secs, 0.5)));
  });
  const double scan_secs = measure("writer during snapshot scan", scan_once);
  std::printf("%-28s %llu rows in %.3f s (%.0f rows/s)\n", "snapshot scan",
              static_cast<unsigned long long>(rows), scan_secs,
              static_cast<double>(rows) / scan_secs);
  measure("writer alone (after)", [&] {
    std::this_thread::sleep_for(std::chrono::duration<double>(std::max(scan_secs, 0.5)));
  });

  stop_writer.store(true);
  writer.join();
}

int RunBench(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&] { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
    if (a == "--engine") {
      auto kind = eg::ParseKind(next());
      if (!kind.IsOk() || !eg::Available(*kind)) {
        std::fprintf(stderr, "--engine: %s\n",
                     kind.IsOk() ? "not part of this build" : kind.GetStatus().ToString().c_str());
        return 2;
      }
      cfg.engine = *kind;
    } else if (a == "--keys") {
      cfg.keys = std::atoi(next().c_str());
    } else if (a == "--value-size") {
      cfg.value_size = static_cast<size_t>(std::atoi(next().c_str()));
    } else if (a == "--seconds") {
      cfg.seconds = std::atoi(next().c_str());
    } else if (a == "--threads") {
      cfg.threads = std::atoi(next().c_str());
    } else if (a == "--workloads") {
      cfg.workloads.clear();
      std::string list = next();
      size_t pos = 0;
      while (pos <= list.size()) {
        const size_t comma = list.find(',', pos);
        cfg.workloads.push_back(list.substr(pos, comma == std::string::npos ? comma : comma - pos));
        if (comma == std::string::npos) {
          break;
        }
        pos = comma + 1;
      }
    } else {
      std::fprintf(stderr, "unknown argument %s\n", a.c_str());
      return 2;
    }
  }
  const auto wants = [&](const char* name) {
    return std::find(cfg.workloads.begin(), cfg.workloads.end(), name) != cfg.workloads.end();
  };

  const auto dir = std::filesystem::temp_directory_path() /
                   ("raftkv_engine_bench_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);

  eg::Options options;
  options.dir = dir;
  options.kind = cfg.engine;
  const auto open_begin = Clock::now();
  auto opened = eg::Open(options);
  const double open_ms = Millis(Clock::now() - open_begin);
  if (!opened.IsOk()) {
    std::fprintf(stderr, "open: %s\n", opened.GetStatus().ToString().c_str());
    return 1;
  }
  auto engine = opened.TakeValue();

  std::printf("# engine benchmark\n");
  std::printf("# engine: %s   keys=%d value=%zu B   %d s per measurement   sync_on_write=off\n",
              std::string(eg::KindName(cfg.engine)).c_str(), cfg.keys, cfg.value_size, cfg.seconds);
  std::printf("# open of an empty directory: %.2f ms\n", open_ms);
  Preload(engine.get(), cfg);
  (void)engine->SyncWal();
  std::printf("# preloaded %d keys (+1/3 overwritten)\n\n", cfg.keys);

  if (wants("apply")) {
    RunApply(engine.get(), cfg);
  }
  if (wants("get")) {
    RunGet(engine.get(), cfg, 1);
    RunGet(engine.get(), cfg, cfg.threads);
  }
  if (wants("scan")) {
    RunScan(engine.get(), cfg);
  }
  if (wants("snapshot")) {
    RunSnapshot(engine.get(), cfg);
  }
  const std::string stats = engine->DescribeStats();
  if (!stats.empty()) {
    std::printf("\n# engine counters: %s\n", stats.c_str());
  }

  engine.reset();
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return RunBench(argc, argv);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "engine_bench: %s\n", e.what());
    return 1;
  }
}
