// Two independent 3-node Raft groups, each owning half the key space, driven
// through one ShardedClient. The point is that routing is right (a key lands
// on its owning group and nowhere else) and that scans stitch the groups back
// into one ordered stream.

#include "client/sharded_client.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

namespace raftkv::client {
namespace {

using server::testing::GrpcClusterFixture;

class ShardedClientTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> AddressesOf(const std::vector<uint64_t>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (uint64_t id : ids) {
      out.push_back(nodes_.at(id).address);
    }
    return out;
  }

  // Shard 1 owns [ "", "m" ), shard 2 owns [ "m", unbounded ).
  void StartTwoShards() {
    StartCluster({1, 2, 3});
    StartCluster({4, 5, 6});
    ASSERT_NE(WaitForLeaderAmong({1, 2, 3}), 0U);
    ASSERT_NE(WaitForLeaderAmong({4, 5, 6}), 0U);

    auto map = shard::ShardMap::Create({{1, "", AddressesOf({1, 2, 3})},
                                        {2, "m", AddressesOf({4, 5, 6})}});
    ASSERT_TRUE(map.IsOk());
    ClientOptions base;
    base.client_id = "test";
    base.jitter_seed = 7;
    client_ = std::make_unique<ShardedClient>(map.TakeValue(), base);
  }

  bool GroupHas(const std::vector<uint64_t>& ids, const std::string& key) {
    for (uint64_t id : ids) {
      auto got = nodes_.at(id).raw_sm->Get(key);
      if (got.IsOk() && got->has_value()) {
        return true;
      }
    }
    return false;
  }

  std::unique_ptr<ShardedClient> client_;
};

TEST_F(ShardedClientTest, KeysLandOnlyOnTheGroupThatOwnsThem) {
  StartTwoShards();
  ASSERT_TRUE(client_->Put("apple", "1").IsOk());
  ASSERT_TRUE(client_->Put("zebra", "2").IsOk());

  auto apple = client_->Get("apple");
  auto zebra = client_->Get("zebra");
  ASSERT_TRUE(apple.IsOk() && apple->has_value());
  ASSERT_TRUE(zebra.IsOk() && zebra->has_value());
  EXPECT_EQ(**apple, "1");
  EXPECT_EQ(**zebra, "2");

  // Followers apply asynchronously, so check the leaders' own state machines.
  const uint64_t lead1 = WaitForLeaderAmong({1, 2, 3});
  const uint64_t lead2 = WaitForLeaderAmong({4, 5, 6});
  EXPECT_TRUE(GroupHas({lead1}, "apple"));
  EXPECT_FALSE(GroupHas({lead1}, "zebra"));
  EXPECT_TRUE(GroupHas({lead2}, "zebra"));
  EXPECT_FALSE(GroupHas({lead2}, "apple"));
}

TEST_F(ShardedClientTest, ScanStitchesShardsIntoOneOrderedStream) {
  StartTwoShards();
  const std::vector<std::string> keys = {"b", "d", "k", "l", "m", "p", "t", "z"};
  for (const auto& k : keys) {
    ASSERT_TRUE(client_->Put(k, "v-" + k).IsOk());
  }

  auto all = client_->Scan("", "");
  ASSERT_TRUE(all.IsOk()) << all.GetStatus().ToString();
  ASSERT_EQ(all->pairs.size(), keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_EQ(all->pairs[i].first, keys[i]);
  }
  EXPECT_FALSE(all->more);

  // A range that straddles the boundary at "m".
  auto mid = client_->Scan("k", "p");
  ASSERT_TRUE(mid.IsOk());
  ASSERT_EQ(mid->pairs.size(), 3U);  // k, l, m
  EXPECT_EQ(mid->pairs.front().first, "k");
  EXPECT_EQ(mid->pairs.back().first, "m");
}

TEST_F(ShardedClientTest, ScanLimitStopsAcrossAShardBoundary) {
  StartTwoShards();
  for (const char* k : {"a", "b", "c", "m", "n", "o"}) {
    ASSERT_TRUE(client_->Put(k, "v").IsOk());
  }
  auto page = client_->Scan("", "", 4);
  ASSERT_TRUE(page.IsOk());
  ASSERT_EQ(page->pairs.size(), 4U);
  EXPECT_EQ(page->pairs.back().first, "m");
  EXPECT_TRUE(page->more);

  auto exact = client_->Scan("", "", 6);
  ASSERT_TRUE(exact.IsOk());
  EXPECT_EQ(exact->pairs.size(), 6U);
  EXPECT_FALSE(exact->more);
}

TEST_F(ShardedClientTest, DeleteAndCasRouteToTheOwningShard) {
  StartTwoShards();
  ASSERT_TRUE(client_->Put("q", "1").IsOk());
  auto cas = client_->Cas("q", std::string("1"), "2");
  ASSERT_TRUE(cas.IsOk());
  EXPECT_TRUE(cas->applied);
  ASSERT_TRUE(client_->Delete("q").IsOk());
  auto gone = client_->Get("q");
  ASSERT_TRUE(gone.IsOk());
  EXPECT_FALSE(gone->has_value());
}

TEST_F(ShardedClientTest, AnOlderMapNeverReplacesANewerOne) {
  StartTwoShards();
  EXPECT_EQ(client_->MapEpoch(), 1U);
  auto newer = shard::ShardMap::Create({{1, "", AddressesOf({1, 2, 3})},
                                        {2, "x", AddressesOf({4, 5, 6})}}, 5);
  ASSERT_TRUE(newer.IsOk());
  client_->UpdateMap(newer.TakeValue());
  EXPECT_EQ(client_->MapEpoch(), 5U);
  EXPECT_EQ(client_->ShardFor("m"), 1U);  // "m" now belongs to the lower shard

  auto stale = shard::ShardMap::Create({{1, "", AddressesOf({1, 2, 3})}}, 2);
  ASSERT_TRUE(stale.IsOk());
  client_->UpdateMap(stale.TakeValue());
  EXPECT_EQ(client_->MapEpoch(), 5U);
}

}  // namespace
}  // namespace raftkv::client
