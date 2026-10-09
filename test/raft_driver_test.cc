// RaftDriver in isolation: one node, no network, a state machine that watches
// how it is called.
//
// The properties here are about the apply step's discipline, which the cluster
// tests only exercise indirectly and, as it turned out, too weakly to notice
// when it was broken. A state machine that is slow inside Apply() is what turns
// a latent ordering hazard into a visible one, so the fake below is
// deliberately slow and deliberately suspicious.

#include "server/raft_driver.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kv.pb.h"

namespace raftkv::server {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// Records every call to Apply and flags the two ways the apply step can be
// misused: two calls overlapping in time, and an index that is not strictly
// greater than the previous one.
class WatchingStateMachine final : public statemachine::StateMachine {
 public:
  explicit WatchingStateMachine(std::chrono::microseconds apply_cost) : apply_cost_(apply_cost) {}

  statemachine::ApplyResult Apply(uint64_t index, std::span<const std::byte> /*command*/) override {
    if (in_apply_.exchange(true)) {
      overlapped_.store(true);
    }
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      if (index <= last_index_) {
        out_of_order_.store(true);
      }
      last_index_ = index;
      ++applied_count_;
    }
    std::this_thread::sleep_for(apply_cost_);  // widen the window for a race
    in_apply_.store(false);

    statemachine::ApplyResult result;
    result.status = statemachine::Status::Ok();
    return result;
  }

  lsm::Result<std::unique_ptr<statemachine::Snapshot>> TakeSnapshot(uint64_t, uint64_t) override {
    return lsm::Status::NotSupported("not used");
  }
  lsm::Status RestoreSnapshot(statemachine::Snapshot&) override {
    return lsm::Status::NotSupported("not used");
  }
  uint64_t LastAppliedIndex() const override {
    const std::lock_guard<std::mutex> lock(mutex_);
    return last_index_;
  }
  lsm::Status Sync() override { return lsm::Status::Ok(); }
  lsm::Result<std::optional<std::string>> Get(std::string_view) const override {
    return std::optional<std::string>{};
  }

  bool Overlapped() const { return overlapped_.load(); }
  bool OutOfOrder() const { return out_of_order_.load(); }
  uint64_t AppliedCount() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return applied_count_;
  }

 private:
  const std::chrono::microseconds apply_cost_;
  mutable std::mutex mutex_;
  uint64_t last_index_ = 0;
  uint64_t applied_count_ = 0;
  std::atomic<bool> in_apply_{false};
  std::atomic<bool> overlapped_{false};
  std::atomic<bool> out_of_order_{false};
};

std::string Put(const std::string& client, uint64_t seq, const std::string& key) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  c.mutable_put()->set_key(key);
  c.mutable_put()->set_value("v");
  return c.SerializeAsString();
}

class RaftDriverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static int counter = 0;
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_driver_" + std::to_string(::getpid()) + "_" + std::to_string(counter++));
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    if (driver_) {
      driver_->Stop();
    }
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  // A one-node cluster: it elects itself and commits on its own, so every
  // proposal drains and applies immediately, from whichever thread proposed.
  void StartSingleNode(std::shared_ptr<statemachine::StateMachine> sm) {
    DriverOptions options;
    options.raft_config.id = 1;
    options.raft_config.peers = {1};
    options.raft_config.election_tick = 3;
    options.raft_config.random_seed = 1;
    options.storage_dir = dir_;
    options.tick_interval_ms = 5;
    auto opened = RaftDriver::Open(std::move(options), std::move(sm));
    ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    driver_ = std::shared_ptr<RaftDriver>(opened.TakeValue().release());
    driver_->Start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (driver_->CurrentRole() != raft::Role::kLeader &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_EQ(driver_->CurrentRole(), raft::Role::kLeader);
  }

  std::filesystem::path dir_;
  std::shared_ptr<RaftDriver> driver_;
};

// Committed entries are handed to the state machine one call at a time, in
// index order, no matter how many threads are proposing at once. Several
// threads drain consecutive batches and then race to apply them; without a
// single ordered path, a later batch can overtake an earlier one and the state
// machine, which treats any index at or below its last as a duplicate, silently
// drops the earlier entry.
TEST_F(RaftDriverTest, ApplyIsSerialisedAndOrderedUnderConcurrentProposals) {
  // The proposer holds the driver's lock across an fsync, so one thread's apply
  // can only overlap the next thread's drain if Apply() takes LONGER than that
  // fsync. A cost below it makes this test pass even with the ordering removed
  // (checked by mutation), so the cost here is deliberately large.
  auto sm = std::make_shared<WatchingStateMachine>(std::chrono::milliseconds(30));
  StartSingleNode(sm);

  constexpr int kThreads = 4;
  constexpr int kPerThread = 8;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      const std::string client = "c" + std::to_string(t);
      for (int i = 0; i < kPerThread; ++i) {
        const std::string cmd =
            Put(client, static_cast<uint64_t>(i) + 1, client + "-" + std::to_string(i));
        auto r = driver_->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(10));
        if (!r.IsOk()) {
          ++failures;
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  ASSERT_EQ(failures.load(), 0);
  EXPECT_FALSE(sm->Overlapped()) << "two Apply calls ran at the same time";
  EXPECT_FALSE(sm->OutOfOrder()) << "Apply was called with a non-increasing index";
  EXPECT_EQ(sm->AppliedCount(), static_cast<uint64_t>(kThreads * kPerThread))
      << "every command must be applied exactly once";
  EXPECT_EQ(driver_->RedeliveredEntries(), 0U);
}

// Each committed entry is delivered once. The failure this guards against is
// subtle: the state machine's own dedup absorbs repeats, so the only symptoms
// are wasted work and an applied index that moves backwards.
TEST_F(RaftDriverTest, TheAppliedIndexNeverMovesBackwards) {
  auto sm = std::make_shared<WatchingStateMachine>(std::chrono::microseconds(0));
  StartSingleNode(sm);

  uint64_t previous = 0;
  for (int i = 0; i < 60; ++i) {
    const std::string cmd = Put("c", static_cast<uint64_t>(i) + 1, "k" + std::to_string(i));
    ASSERT_TRUE(driver_->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(5)).IsOk());
    const uint64_t now = driver_->AppliedRaftIndex();
    ASSERT_GE(now, previous) << "applied index went from " << previous << " to " << now;
    previous = now;
  }
  EXPECT_EQ(driver_->RedeliveredEntries(), 0U);
  EXPECT_EQ(sm->AppliedCount(), 60U);
}

}  // namespace
}  // namespace raftkv::server
