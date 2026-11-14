// The balancer against real Raft groups: a directory, one shard that grows,
// and a spare group that starts empty and is handed out when a split is due.

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/shard_balancer.h"
#include "client/sharded_client.h"
#include "grpc_cluster_fixture.h"

namespace raftkv::client {
namespace {

using server::testing::GrpcClusterFixture;

std::string Key(int i) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "key%04d", i);
  return buf;
}

class ShardBalancerTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> AddressesOf(const std::vector<uint64_t>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (uint64_t id : ids) out.push_back(nodes_.at(id).address);
    return out;
  }

  ClientOptions Base(const std::string& name) const {
    ClientOptions o;
    o.client_id = name;
    o.jitter_seed = 5;
    return o;
  }

  // Directory {7,8,9}; shard 1 {1,2,3} owns everything; spare group {4,5,6}.
  void Start(size_t max_keys) {
    StartCluster({7, 8, 9});
    StartCluster({1, 2, 3});
    StartCluster({4, 5, 6});
    for (const auto& g : std::vector<std::vector<uint64_t>>{{7, 8, 9}, {1, 2, 3}, {4, 5, 6}}) {
      ASSERT_NE(WaitForLeaderAmong(g), 0U);
    }
    ClientOptions meta = Base("dir");
    meta.addresses = AddressesOf({7, 8, 9});
    directory_ = std::make_unique<ShardDirectory>(meta);
    auto map = shard::ShardMap::Create({{1, "", AddressesOf({1, 2, 3})}});
    ASSERT_TRUE(map.IsOk());
    auto published = directory_->CompareAndPublish(std::nullopt, *map);
    ASSERT_TRUE(published.IsOk() && *published);

    client_ = std::make_unique<ShardedClient>(*map, Base("app"));
    client_->SetDirectory(directory_.get());

    BalancerOptions options;
    options.max_keys_per_shard = max_keys;
    options.scan_page = 50;
    balancer_ = std::make_unique<ShardBalancer>(client_.get(), directory_.get(), Base("balancer"), options);
  }

  void Fill(int n) {
    for (int i = 0; i < n; ++i) {
      const Status put = client_->Put(Key(i), "v" + std::to_string(i));
      ASSERT_TRUE(put.IsOk()) << Key(i) << ": " << put.ToString();
    }
  }

  std::unique_ptr<ShardDirectory> directory_;
  std::unique_ptr<ShardedClient> client_;
  std::unique_ptr<ShardBalancer> balancer_;
};

TEST_F(ShardBalancerTest, ASmallShardIsLeftAlone) {
  Start(/*max_keys=*/100);
  Fill(60);
  auto plan = balancer_->Plan();
  ASSERT_TRUE(plan.IsOk()) << plan.GetStatus().ToString();
  EXPECT_FALSE(plan->has_value());

  int provisioned = 0;
  auto did = balancer_->RunOnce([&]() -> Result<std::vector<std::string>> {
    ++provisioned;
    return AddressesOf({4, 5, 6});
  });
  ASSERT_TRUE(did.IsOk());
  EXPECT_FALSE(*did);
  EXPECT_EQ(provisioned, 0) << "a spare group must not be consumed when nothing is split";
}

TEST_F(ShardBalancerTest, AnOversizedShardIsSplitAtItsMedian) {
  Start(/*max_keys=*/100);
  Fill(160);

  auto plan = balancer_->Plan();
  ASSERT_TRUE(plan.IsOk()) << plan.GetStatus().ToString();
  ASSERT_TRUE(plan->has_value());
  EXPECT_EQ((*plan)->source_shard, 1U);
  EXPECT_GT((*plan)->keys_counted, 100U);

  auto did = balancer_->RunOnce(
      [&]() -> Result<std::vector<std::string>> { return AddressesOf({4, 5, 6}); });
  ASSERT_TRUE(did.IsOk()) << did.GetStatus().ToString();
  ASSERT_TRUE(*did);

  // Two shards now, cut near the middle of what was counted.
  auto map = directory_->Load();
  ASSERT_TRUE(map.IsOk() && map->has_value());
  ASSERT_EQ((*map)->Ranges().size(), 2U);
  EXPECT_EQ((*map)->Ranges()[1].shard_id, 2U);
  EXPECT_EQ((*map)->Ranges()[1].start, (*plan)->split_key);

  // Nothing was lost or duplicated, and the data really is on both sides.
  auto all = client_->Scan("", "");
  ASSERT_TRUE(all.IsOk()) << all.GetStatus().ToString();
  ASSERT_EQ(all->pairs.size(), 160U);
  for (int i = 0; i < 160; ++i) {
    EXPECT_EQ(all->pairs[static_cast<size_t>(i)].first, Key(i));
    EXPECT_EQ(all->pairs[static_cast<size_t>(i)].second, "v" + std::to_string(i));
  }
  EXPECT_NE(client_->ShardFor(Key(5)), client_->ShardFor(Key(155)));

  // Each half is now under the limit, so a second pass does nothing.
  auto again = balancer_->Plan();
  ASSERT_TRUE(again.IsOk()) << again.GetStatus().ToString();
  EXPECT_FALSE(again->has_value()) << "split key " << (again->has_value() ? (*again)->split_key : "");
}

TEST_F(ShardBalancerTest, WritesContinueWhileTheBalancerSplits) {
  Start(/*max_keys=*/100);
  Fill(150);

  std::atomic<bool> stop{false};
  std::atomic<int> acknowledged{0};
  std::atomic<int> failures{0};
  std::thread writer([&] {
    ShardedClient own(*directory_->Load().TakeValue(), Base("writer"));
    own.SetDirectory(directory_.get());
    for (int i = 1000; !stop.load() && i < 1400; ++i) {
      if (own.Put(Key(i), "w").IsOk()) {
        ++acknowledged;
      } else {
        ++failures;
      }
    }
  });

  auto did = balancer_->RunOnce(
      [&]() -> Result<std::vector<std::string>> { return AddressesOf({4, 5, 6}); });
  stop.store(true);
  writer.join();
  ASSERT_TRUE(did.IsOk()) << did.GetStatus().ToString();
  EXPECT_TRUE(*did);
  EXPECT_EQ(failures.load(), 0);

  // Every acknowledged write is readable afterwards.
  auto all = client_->Scan("", "");
  ASSERT_TRUE(all.IsOk());
  EXPECT_EQ(all->pairs.size(), static_cast<size_t>(150 + acknowledged.load()));
}

}  // namespace
}  // namespace raftkv::client
