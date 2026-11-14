#pragma once

// The client side of a linearizability campaign, shared by the in-process and
// the real-process tests: concurrent clients issue puts, gets and
// compare-and-swaps on a few keys and record each operation with real-time
// stamps. An operation that fails is recorded with an unknown outcome.

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "linearizability.h"

namespace raftkv::testing {

inline uint64_t NowNanos() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

struct WorkloadConfig {
  int threads = 5;
  int ops_per_thread = 220;
  int keys = 5;
  uint64_t seed = 1;
};

// `make_options(seed)` supplies the client options for each operation (a fresh
// client per operation, so addresses can change under it).
inline std::vector<HistoryOp> RunWorkload(
    const WorkloadConfig& c, const std::function<client::ClientOptions(uint64_t)>& make_options) {
  namespace rk = raftkv::client;
  std::mutex history_mutex;
  std::vector<HistoryOp> history;
  const auto& Options = make_options;
  const auto Now = NowNanos;

    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(c.threads));
    for (int t = 0; t < c.threads; ++t) {
      workers.emplace_back([&, t] {
        std::mt19937_64 rng(c.seed * 104729 + static_cast<uint64_t>(t));
        std::vector<HistoryOp> mine;
        // The last value this thread wrote or read per key: the natural thing to
        // hand to compare-and-swap, so a fair share of swaps can succeed.
        std::map<std::string, std::string> known;
        for (int n = 0; n < c.ops_per_thread; ++n) {
          HistoryOp op;
          op.thread = t;
          op.key = "k" + std::to_string(rng() % static_cast<uint64_t>(c.keys));
          const uint64_t pick = rng() % 10;
          rk::KvClient client(Options(rng()));
          const std::string value = "t" + std::to_string(t) + "-" + std::to_string(n);
          if (pick < 4) {
            op.kind = RegOp::kPut;
            op.value = value;
            op.invoke = Now();
            const auto s = client.Put(op.key, op.value);
            op.ret = Now();
            op.completed = s.IsOk();
            known[op.key] = op.value;
          } else if (pick < 8) {
            op.kind = RegOp::kGet;
            op.invoke = Now();
            auto r = client.Get(op.key);
            op.ret = Now();
            op.completed = r.IsOk();
            if (r.IsOk()) {
              op.observed = *r;
              if (*r) known[op.key] = **r;
            }
          } else {
            op.kind = RegOp::kCas;
            op.value = value;
            // Expect a value someone plausibly wrote, or absence.
            const uint64_t how = rng() % 4;
            if (how == 0) {
              op.expected = std::nullopt;
            } else if (how <= 2 && known.count(op.key) != 0) {
              op.expected = known[op.key];
            } else {
              op.expected = "t" + std::to_string(rng() % static_cast<uint64_t>(c.threads)) + "-" +
                            std::to_string(rng() % static_cast<uint64_t>(c.ops_per_thread));
            }
            op.invoke = Now();
            auto r = client.Cas(op.key, op.expected, op.value);
            op.ret = Now();
            op.completed = r.IsOk();
            if (r.IsOk()) {
              op.applied = r->applied;
              if (!r->applied) op.observed = r->current;
              if (r->applied) known[op.key] = op.value;
            }
          }
          if (!op.completed) op.ret = std::numeric_limits<uint64_t>::max();
          mine.push_back(std::move(op));
        }
        const std::lock_guard<std::mutex> lock(history_mutex);
        for (auto& op : mine) history.push_back(std::move(op));
      });
    }
  for (auto& w : workers) w.join();
  return history;
}

inline std::string DescribeFailure(const CheckResult& r) {
    if (r.linearizable) return {};
    std::string text = "history for key " + r.failing_key + " has no valid order:\n";
    auto ops = r.failing_history;
    std::sort(ops.begin(), ops.end(), [](const HistoryOp& a, const HistoryOp& b) { return a.invoke < b.invoke; });
    const uint64_t base = ops.empty() ? 0 : ops.front().invoke;
    for (const auto& o : ops) {
      const char* kind = o.kind == RegOp::kPut ? "put" : o.kind == RegOp::kGet ? "get" : "cas";
      text += "  [" + std::to_string((o.invoke - base) / 1000) + "us, " +
              (o.completed ? std::to_string((o.ret - base) / 1000) + "us" : std::string("?")) + "] t" +
              std::to_string(o.thread) + " " + kind + " ";
      if (o.kind == RegOp::kGet) {
        text += o.completed ? (o.observed ? *o.observed : std::string("<absent>")) : std::string("<unknown>");
      } else {
        text += o.value;
        if (o.kind == RegOp::kCas) {
          text += " if " + (o.expected ? *o.expected : std::string("<absent>")) +
                  (o.completed ? (o.applied ? " -> applied" : " -> refused, found " +
                                                                  (o.observed ? *o.observed : std::string("<absent>")))
                               : std::string(" -> unknown"));
        }
      }
      text += "\n";
    }
    return text;
  }


}  // namespace raftkv::testing
