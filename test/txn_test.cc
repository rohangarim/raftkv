// Cross-shard transactions against two real Raft groups.
//
// The properties that matter are the ones that break first when two-phase
// commit is wrong: money is never created or destroyed, a reader never sees half
// of a committed transaction, and a coordinator that dies at the worst moment
// leaves the data either fully applied or fully not.

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

#include "client/sharded_client.h"

namespace raftkv::client {
namespace {

using server::testing::GrpcClusterFixture;

class TxnTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> AddressesOf(const std::vector<uint64_t>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (uint64_t id : ids) {
      out.push_back(nodes_.at(id).address);
    }
    return out;
  }

  // Shard 1 owns keys below "acct-m", shard 2 owns "acct-m" and up, so "acct-a"
  // and "acct-b" live on one shard and "acct-y" and "acct-z" on the other.
  void StartTwoShards() {
    StartCluster({1, 2, 3});
    StartCluster({4, 5, 6});
    ASSERT_NE(WaitForLeaderAmong({1, 2, 3}), 0U);
    ASSERT_NE(WaitForLeaderAmong({4, 5, 6}), 0U);
    auto map = shard::ShardMap::Create(
        {{1, "", AddressesOf({1, 2, 3})}, {2, "acct-m", AddressesOf({4, 5, 6})}});
    ASSERT_TRUE(map.IsOk());
    map_ = std::make_unique<shard::ShardMap>(map.TakeValue());
  }

  std::unique_ptr<ShardedClient> NewClient(const std::string& name) {
    ClientOptions base;
    base.client_id = name;
    base.jitter_seed = 5;
    auto client = std::make_unique<ShardedClient>(*map_, base);
    client->SetLockWait(std::chrono::milliseconds(150));
    return client;
  }

  std::unique_ptr<shard::ShardMap> map_;
};

TEST_F(TxnTest, ACommittedTransactionAppliesEveryWriteOnBothShards) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "100").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "50").IsOk());
  ASSERT_NE(client->ShardFor("acct-a"), client->ShardFor("acct-z"))
      << "the test needs these keys on different shards";

  Transaction txn = client->Begin();
  auto a = txn.Get("acct-a");
  auto z = txn.Get("acct-z");
  ASSERT_TRUE(a.IsOk() && z.IsOk());
  txn.Put("acct-a", std::to_string(std::stoi(**a) - 30));
  txn.Put("acct-z", std::to_string(std::stoi(**z) + 30));
  auto committed = txn.Commit();
  ASSERT_TRUE(committed.IsOk()) << committed.GetStatus().ToString();
  EXPECT_TRUE(*committed);

  EXPECT_EQ(**client->Get("acct-a"), "70");
  EXPECT_EQ(**client->Get("acct-z"), "80");
}

TEST_F(TxnTest, ATransactionWhoseReadWentStaleChangesNothing) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "100").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "50").IsOk());

  Transaction txn = client->Begin();
  ASSERT_TRUE(txn.Get("acct-a").IsOk());
  ASSERT_TRUE(txn.Get("acct-z").IsOk());
  txn.Put("acct-a", "0");
  txn.Put("acct-z", "150");

  // Someone else changes a key the transaction read, after it read it.
  auto other = NewClient("other");
  ASSERT_TRUE(other->Put("acct-z", "999").IsOk());

  auto committed = txn.Commit();
  ASSERT_TRUE(committed.IsOk());
  EXPECT_FALSE(*committed);
  EXPECT_EQ(**client->Get("acct-a"), "100") << "a refused transaction wrote to the other shard";
  EXPECT_EQ(**client->Get("acct-z"), "999");
}

TEST_F(TxnTest, ReadYourOwnWritesAndRepeatableReads) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "1").IsOk());
  Transaction txn = client->Begin();
  EXPECT_EQ(**txn.Get("acct-a"), "1");
  txn.Put("acct-a", "2");
  EXPECT_EQ(**txn.Get("acct-a"), "2");
  txn.Delete("acct-a");
  EXPECT_FALSE(txn.Get("acct-a")->has_value());
  ASSERT_TRUE(*txn.Commit());
  EXPECT_FALSE(client->Get("acct-a")->has_value());
}

TEST_F(TxnTest, ASingleShardTransactionUsesNoTwoPhaseCommit) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("b1", "1").IsOk());
  Transaction txn = client->Begin();
  auto v = txn.Get("b1");
  ASSERT_TRUE(v.IsOk());
  txn.Put("b1", "2");
  txn.Put("b2", "3");
  ASSERT_TRUE(*txn.Commit());
  EXPECT_EQ(**client->Get("b2"), "3");
}

// The classic invariant: transfers between accounts on different shards,
// racing, never change the total.
TEST_F(TxnTest, ConcurrentTransfersAcrossShardsPreserveTheTotal) {
  StartTwoShards();
  constexpr int kAccounts = 4;  // two on each shard
  constexpr int kInitial = 1000;
  std::vector<std::string> names = {"acct-a", "acct-b", "acct-y", "acct-z"};
  {
    auto setup = NewClient("setup");
    for (const auto& n : names) {
      ASSERT_TRUE(setup->Put(n, std::to_string(kInitial)).IsOk());
    }
  }

  std::atomic<int> committed{0};
  std::atomic<int> retries{0};
  std::atomic<int> errors{0};
  std::vector<std::thread> workers;
  workers.reserve(4);
  for (int w = 0; w < 4; ++w) {
    workers.emplace_back([&, w] {
      auto client = NewClient("worker" + std::to_string(w));
      std::mt19937 rng(static_cast<unsigned>(w) + 1);
      for (int i = 0; i < 12; ++i) {
        const std::string& from = names[rng() % kAccounts];
        const std::string& to =
            names[(rng() % (kAccounts - 1) + 1 + (&from - &names[0])) % kAccounts];
        const int amount = static_cast<int>(rng() % 50) + 1;
        for (int attempt = 0; attempt < 50; ++attempt) {
          Transaction txn = client->Begin();
          auto f = txn.Get(from);
          auto t = txn.Get(to);
          if (!f.IsOk() || !t.IsOk()) {
            ++errors;
            break;
          }
          txn.Put(from, std::to_string(std::stoi(**f) - amount));
          txn.Put(to, std::to_string(std::stoi(**t) + amount));
          auto done = txn.Commit();
          if (!done.IsOk()) {
            ++errors;
            break;
          }
          if (*done) {
            ++committed;
            break;
          }
          ++retries;
        }
      }
    });
  }
  for (auto& t : workers) t.join();

  EXPECT_EQ(errors.load(), 0);
  EXPECT_EQ(committed.load(), 48) << "some transfers never went through";
  auto check = NewClient("check");
  int total = 0;
  for (const auto& n : names) {
    auto v = check->Get(n);
    ASSERT_TRUE(v.IsOk() && v->has_value());
    total += std::stoi(**v);
  }
  EXPECT_EQ(total, kAccounts * kInitial) << "money was created or destroyed";
  std::printf("[txn] %d transfers committed, %d retried after a conflict\n", committed.load(),
              retries.load());
}

// A transaction that reads both accounts and validates must never commit a
// view in which only half of a transfer is visible.
TEST_F(TxnTest, ReadersNeverSeeHalfOfATransfer) {
  StartTwoShards();
  {
    auto setup = NewClient("setup");
    ASSERT_TRUE(setup->Put("acct-a", "500").IsOk());
    ASSERT_TRUE(setup->Put("acct-z", "500").IsOk());
  }
  std::atomic<bool> stop{false};
  std::atomic<int> bad_views{0};
  std::atomic<int> views{0};

  std::thread writer([&] {
    auto client = NewClient("writer");
    int i = 0;
    while (!stop.load()) {
      Transaction txn = client->Begin();
      auto a = txn.Get("acct-a");
      auto z = txn.Get("acct-z");
      if (!a.IsOk() || !z.IsOk()) continue;
      const int delta = (i++ % 2 == 0) ? 7 : -7;
      txn.Put("acct-a", std::to_string(std::stoi(**a) - delta));
      txn.Put("acct-z", std::to_string(std::stoi(**z) + delta));
      (void)txn.Commit();
    }
  });
  std::thread reader([&] {
    auto client = NewClient("reader");
    while (!stop.load()) {
      Transaction txn = client->Begin();
      auto a = txn.Get("acct-a");
      auto z = txn.Get("acct-z");
      if (!a.IsOk() || !z.IsOk()) continue;
      auto ok = txn.Commit();  // validates that both reads still hold together
      if (ok.IsOk() && *ok) {
        ++views;
        if (std::stoi(**a) + std::stoi(**z) != 1000) ++bad_views;
      }
    }
  });
  std::this_thread::sleep_for(std::chrono::seconds(3));
  stop = true;
  writer.join();
  reader.join();
  EXPECT_GT(views.load(), 0);
  EXPECT_EQ(bad_views.load(), 0) << "a validated read saw a torn transfer";
}

// Coordinator dies with every lock taken and nothing decided. The transaction
// must simply not have happened, and the keys must free up.
TEST_F(TxnTest, ACoordinatorThatDiesBeforeDecidingLeavesNoTrace) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "100").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "100").IsOk());

  {
    auto doomed = NewClient("doomed");
    doomed->txn_stop_after = [](const std::string& step) { return step == "prewritten"; };
    Transaction txn = doomed->Begin();
    ASSERT_TRUE(txn.Get("acct-a").IsOk());
    ASSERT_TRUE(txn.Get("acct-z").IsOk());
    txn.Put("acct-a", "0");
    txn.Put("acct-z", "200");
    EXPECT_FALSE(txn.Commit().IsOk());  // "died" here
  }

  // Both keys are locked. A reader is not blocked forever: it settles the
  // abandoned transaction and sees the original values.
  EXPECT_EQ(**client->Get("acct-a"), "100");
  EXPECT_EQ(**client->Get("acct-z"), "100");
  // And the keys are writable again.
  ASSERT_TRUE(client->Put("acct-a", "101").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "102").IsOk());
}

// Coordinator dies after the decision is durable but before the second shard
// was told. The transaction HAS committed; anyone touching the leftover lock
// must finish it rather than lose it or expose half of it.
TEST_F(TxnTest, ACoordinatorThatDiesAfterDecidingIsRolledForwardByTheNextReader) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "100").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "100").IsOk());

  {
    auto doomed = NewClient("doomed");
    doomed->txn_stop_after = [](const std::string& step) { return step == "primary-committed"; };
    Transaction txn = doomed->Begin();
    ASSERT_TRUE(txn.Get("acct-a").IsOk());
    ASSERT_TRUE(txn.Get("acct-z").IsOk());
    txn.Put("acct-a", "40");
    txn.Put("acct-z", "160");
    EXPECT_FALSE(txn.Commit().IsOk());
  }

  // The primary is "acct-a" (the smallest key). Reading the SECOND shard's key
  // first meets a lock whose transaction has committed: it must roll forward.
  EXPECT_EQ(**client->Get("acct-z"), "160");
  EXPECT_EQ(**client->Get("acct-a"), "40");
}

TEST_F(TxnTest, AScanThatMeetsALeftoverLockSettlesItFirst) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "1").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "1").IsOk());
  {
    auto doomed = NewClient("doomed");
    doomed->txn_stop_after = [](const std::string& step) { return step == "primary-committed"; };
    Transaction txn = doomed->Begin();
    ASSERT_TRUE(txn.Get("acct-a").IsOk());
    ASSERT_TRUE(txn.Get("acct-z").IsOk());
    txn.Put("acct-a", "2");
    txn.Put("acct-z", "2");
    EXPECT_FALSE(txn.Commit().IsOk());
  }
  auto all = client->Scan("", "");
  ASSERT_TRUE(all.IsOk()) << all.GetStatus().ToString();
  ASSERT_EQ(all->pairs.size(), 2U);
  EXPECT_EQ(all->pairs[0].second, "2");
  EXPECT_EQ(all->pairs[1].second, "2")
      << "the scan returned the pre-commit value of a committed key";
}

TEST_F(TxnTest, APlainWriteWaitsOutAnAbandonedTransactionInsteadOfFailing) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "1").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "1").IsOk());
  {
    auto doomed = NewClient("doomed");
    doomed->txn_stop_after = [](const std::string& step) { return step == "prewritten"; };
    Transaction txn = doomed->Begin();
    ASSERT_TRUE(txn.Get("acct-a").IsOk());
    txn.Put("acct-a", "x");
    txn.Put("acct-z", "y");
    EXPECT_FALSE(txn.Commit().IsOk());
  }
  const auto start = std::chrono::steady_clock::now();
  ASSERT_TRUE(client->Put("acct-z", "mine").IsOk());
  EXPECT_EQ(**client->Get("acct-z"), "mine");
  EXPECT_EQ(**client->Get("acct-a"), "1") << "the abandoned transaction partly applied";
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(5));
}

TEST_F(TxnTest, CommittingAnAlreadyRolledBackTransactionFails) {
  StartTwoShards();
  auto client = NewClient("c");
  ASSERT_TRUE(client->Put("acct-a", "1").IsOk());
  ASSERT_TRUE(client->Put("acct-z", "1").IsOk());
  {
    auto doomed = NewClient("doomed");
    doomed->txn_stop_after = [](const std::string& step) { return step == "prewritten"; };
    Transaction txn = doomed->Begin();
    ASSERT_TRUE(txn.Get("acct-a").IsOk());
    ASSERT_TRUE(txn.Get("acct-z").IsOk());
    txn.Put("acct-a", "9");
    txn.Put("acct-z", "9");
    EXPECT_FALSE(txn.Commit().IsOk());
  }
  // A reader rolls it back; later reads are consistent with that.
  EXPECT_EQ(**client->Get("acct-a"), "1");
  EXPECT_EQ(**client->Get("acct-z"), "1");
  EXPECT_EQ(**client->Get("acct-a"), "1");
}

}  // namespace
}  // namespace raftkv::client
