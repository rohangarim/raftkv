// A randomized campaign against a real 3-node cluster: concurrent clients
// issue puts, gets and compare-and-swaps on a handful of keys over gRPC while a
// chaos thread repeatedly kills nodes (leaders included) and restarts them on
// their old directories. Every operation is recorded with real-time stamps and
// the whole history is checked for linearizability.
//
// An operation that fails or times out is recorded as "outcome unknown", which
// the checker treats as "may or may not have taken effect, at any point after
// it started". That is what a real client knows after a timeout.
//
// RAFTKV_LIN_SEED (default 1) picks the schedule's random seed, so a failure
// can be replayed and a longer campaign is a loop over seeds.

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <mutex>
#include <random>
#include <thread>

#include <gtest/gtest.h>

#include "client/kv_client.h"
#include "grpc_cluster_fixture.h"
#include "linearizability.h"

namespace raftkv::testing {
namespace {

using Clock = std::chrono::steady_clock;
using raftkv::server::testing::GrpcClusterFixture;
namespace rk = raftkv::client;

uint64_t Now() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   Clock::now().time_since_epoch())
                                   .count());
}

class LinearizabilityClusterTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> LiveAddresses() {
    const std::lock_guard<std::mutex> lock(addr_mutex_);
    return addresses_;
  }
  void PublishAddresses(const std::vector<uint64_t>& up) {
    const std::lock_guard<std::mutex> lock(addr_mutex_);
    addresses_.clear();
    for (uint64_t id : up) addresses_.push_back(nodes_.at(id).address);
  }

  rk::ClientOptions Options(uint64_t seed) {
    rk::ClientOptions o;
    o.addresses = LiveAddresses();
    o.client_id = "lin-" + std::to_string(id_counter_.fetch_add(1));
    o.attempt_timeout = std::chrono::milliseconds(600);
    o.total_timeout = std::chrono::milliseconds(1500);
    o.jitter_seed = seed;
    return o;
  }

  struct Campaign {
    int threads = 5;
    int ops_per_thread = 220;
    int keys = 5;
    int crashes = 8;
    uint64_t seed = 1;
  };

  struct Outcome {
    std::vector<HistoryOp> history;
    size_t unknown = 0;
    int crashes_done = 0;
  };

  Outcome RunCampaign(const Campaign& c) {
    const std::vector<uint64_t> ids{1, 2, 3};
    StartCluster(ids);
    EXPECT_NE(WaitForLeader(), 0U);
    PublishAddresses(ids);

    std::mutex history_mutex;
    Outcome out;
    std::atomic<bool> clients_done{false};

    std::thread chaos([&] {
      std::mt19937_64 rng(c.seed * 7919);
      for (int i = 0; i < c.crashes && !clients_done.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150 + rng() % 450));
        if (clients_done.load()) break;
        const uint64_t victim = ids[rng() % ids.size()];
        std::vector<uint64_t> survivors;
        for (uint64_t id : ids) {
          if (id != victim) survivors.push_back(id);
        }
        PublishAddresses(survivors);
        CrashNode(victim);
        // Now and then take a second node too: no quorum, so operations in
        // flight run out of time and their outcome is genuinely unknown.
        uint64_t second = 0;
        if (i % 4 == 3) {
          second = survivors[rng() % survivors.size()];
          PublishAddresses({survivors[0] == second ? survivors[1] : survivors[0]});
          CrashNode(second);
          std::this_thread::sleep_for(std::chrono::milliseconds(1800));
        } else {
          std::this_thread::sleep_for(std::chrono::milliseconds(100 + rng() % 500));
        }
        RestartNode(victim, ids);
        if (second != 0) RestartNode(second, ids);
        PublishAddresses(ids);
        ++out.crashes_done;
      }
    });

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
        for (auto& op : mine) out.history.push_back(std::move(op));
      });
    }
    for (auto& w : workers) w.join();
    clients_done.store(true);
    chaos.join();

    for (const auto& op : out.history) {
      if (!op.completed) ++out.unknown;
    }
    return out;
  }

  static void Report(const CheckResult& r) {
    if (r.linearizable) return;
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
    ADD_FAILURE() << text;
  }

  std::mutex addr_mutex_;
  std::vector<std::string> addresses_;
  std::atomic<uint64_t> id_counter_{0};
};

TEST_F(LinearizabilityClusterTest, HistoryUnderNodeKillsIsLinearizable) {
  Campaign c;
  // NOLINTNEXTLINE(concurrency-mt-unsafe): read once, before any thread starts.
  if (const char* s = std::getenv("RAFTKV_LIN_SEED")) c.seed = std::strtoull(s, nullptr, 10);
  const Outcome out = RunCampaign(c);

  size_t gets = 0;
  size_t cas_applied = 0;
  for (const auto& op : out.history) {
    if (op.kind == RegOp::kGet && op.completed) ++gets;
    if (op.kind == RegOp::kCas && op.completed && op.applied) ++cas_applied;
  }
  std::printf("campaign: %zu ops, %zu unknown, %d node kills, %zu reads, %zu successful swaps\n",
              out.history.size(), out.unknown, out.crashes_done, gets, cas_applied);

  // The run has to have exercised something, or "linearizable" means little.
  EXPECT_GE(out.crashes_done, 1);
  EXPECT_GT(gets, 20U);
  EXPECT_GT(out.unknown, 0U) << "no operation ever timed out, so the unknown-outcome path went untested";
  EXPECT_GT(out.history.size() - out.unknown, out.history.size() / 2) << "cluster was unavailable most of the run";

  const CheckResult r = CheckLinearizable(out.history);
  EXPECT_FALSE(r.inconclusive) << "checker ran out of budget";
  Report(r);
}

}  // namespace
}  // namespace raftkv::testing
