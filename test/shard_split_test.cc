// Live shard splits against real Raft groups: three independent clusters (the
// directory, the source shard, and a target that starts empty).

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/shard_splitter.h"
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

class ShardSplitTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> AddressesOf(const std::vector<uint64_t>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (uint64_t id : ids) {
      out.push_back(nodes_.at(id).address);
    }
    return out;
  }

  ClientOptions Base(const std::string& name) const {
    ClientOptions o;
    o.client_id = name;
    o.jitter_seed = 11;
    return o;
  }

  // Directory {7,8,9}, source shard 1 {1,2,3} owning everything, target {4,5,6}.
  void Start() {
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
  }

  SplitRequest Request(const std::string& id, const std::string& split_key) {
    SplitRequest r;
    r.id = id;
    r.source_shard = 1;
    r.split_key = split_key;
    r.target = {2, split_key, AddressesOf({4, 5, 6})};
    return r;
  }

  // Direct, untracked read of a node's local state, to see where bytes live.
  bool NodeHas(const std::vector<uint64_t>& ids, const std::string& key) {
    for (uint64_t id : ids) {
      auto got = nodes_.at(id).raw_sm->Get(key);
      if (got.IsOk() && got->has_value()) return true;
    }
    return false;
  }

  void WaitForQuiescence() { std::this_thread::sleep_for(std::chrono::milliseconds(400)); }

  std::unique_ptr<ShardDirectory> directory_;
  std::unique_ptr<ShardedClient> client_;
};

TEST_F(ShardSplitTest, SplitMovesTheUpperHalfAndLeavesNothingBehind) {
  Start();
  for (int i = 0; i < 100; ++i) {
    const Status put = client_->Put(Key(i), "v" + std::to_string(i));
    ASSERT_TRUE(put.IsOk()) << Key(i) << ": " << put.ToString();
  }

  ShardSplitter splitter(directory_.get(), Base("admin"));
  const Status s = splitter.Split(Request("s1", Key(50)));
  ASSERT_TRUE(s.IsOk()) << s.ToString();

  // Everything is still readable, through a client that learns the new layout.
  for (int i = 0; i < 100; ++i) {
    auto got = client_->Get(Key(i));
    ASSERT_TRUE(got.IsOk()) << Key(i) << ": " << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << Key(i);
    EXPECT_EQ(**got, "v" + std::to_string(i));
  }
  EXPECT_EQ(client_->MapEpoch(), 2U);
  EXPECT_NE(client_->ShardFor(Key(10)), client_->ShardFor(Key(90)));

  // The bytes really moved: the target has the upper half, the source has none of it.
  WaitForQuiescence();
  EXPECT_TRUE(NodeHas({4, 5, 6}, Key(50)));
  EXPECT_TRUE(NodeHas({4, 5, 6}, Key(99)));
  EXPECT_FALSE(NodeHas({4, 5, 6}, Key(49)));
  EXPECT_TRUE(NodeHas({1, 2, 3}, Key(0)));
  EXPECT_TRUE(NodeHas({1, 2, 3}, Key(49)));
  EXPECT_FALSE(NodeHas({1, 2, 3}, Key(50))) << "the source kept keys it gave away";
  EXPECT_FALSE(NodeHas({1, 2, 3}, Key(99)));

  // A scan across the new boundary is still ordered and complete.
  auto all = client_->Scan("", "");
  ASSERT_TRUE(all.IsOk()) << all.GetStatus().ToString();
  ASSERT_EQ(all->pairs.size(), 100U);
  for (int i = 0; i < 100; ++i) EXPECT_EQ(all->pairs[static_cast<size_t>(i)].first, Key(i));
}

TEST_F(ShardSplitTest, ClientsWithAStaleMapRecoverByRefreshing) {
  Start();
  for (int i = 0; i < 40; ++i) {
    const Status put = client_->Put(Key(i), "old");
    ASSERT_TRUE(put.IsOk()) << Key(i) << ": " << put.ToString();
  }

  // A second client that has not seen the split.
  auto stale_map = directory_->Load();
  ASSERT_TRUE(stale_map.IsOk() && stale_map->has_value());
  ShardedClient stale(**stale_map, Base("stale"));
  stale.SetDirectory(directory_.get());
  ASSERT_EQ(stale.MapEpoch(), 1U);

  ShardSplitter splitter(directory_.get(), Base("admin"));
  ASSERT_TRUE(splitter.Split(Request("s2", Key(20))).IsOk());

  // Its first write goes to the old owner, which now refuses; it must notice,
  // refresh and land the write on the new one.
  ASSERT_TRUE(stale.Put(Key(30), "after-split").IsOk());
  EXPECT_EQ(stale.MapEpoch(), 2U);
  auto got = client_->Get(Key(30));
  ASSERT_TRUE(got.IsOk() && got->has_value());
  EXPECT_EQ(**got, "after-split");
  EXPECT_TRUE(NodeHas({4, 5, 6}, Key(30)));
}

// The property that matters: a writer hammering both halves of the key space
// through the whole split never loses an acknowledged write, and a read never
// returns anything older than the last acknowledged write to that key.
TEST_F(ShardSplitTest, NoAcknowledgedWriteIsLostWhileSplittingUnderLoad) {
  Start();
  constexpr int kKeys = 60;
  for (int i = 0; i < kKeys; ++i) {
    const Status put = client_->Put(Key(i), "0");
    ASSERT_TRUE(put.IsOk()) << Key(i) << ": " << put.ToString();
  }

  std::atomic<bool> stop{false};
  std::mutex mu;
  std::map<int, int> acked;  // key -> highest acknowledged version
  for (int i = 0; i < kKeys; ++i) acked[i] = 0;
  std::atomic<int> failures{0};
  std::atomic<int> stale_reads{0};

  // A separate client for the load, so it exercises routing like an app would.
  auto start_map = directory_->Load();
  ASSERT_TRUE(start_map.IsOk());
  ShardedClient load(**start_map, Base("load"));
  load.SetDirectory(directory_.get());

  std::thread writer([&] {
    int version = 0;
    while (!stop.load()) {
      ++version;
      for (int i = 0; i < kKeys && !stop.load(); i += 3) {
        const Status s = load.Put(Key(i), std::to_string(version));
        if (s.IsOk()) {
          std::lock_guard<std::mutex> lock(mu);
          acked[i] = version;
        } else {
          ++failures;
        }
        int floor = 0;
        {
          std::lock_guard<std::mutex> lock(mu);
          floor = acked[i];
        }
        auto got = load.Get(Key(i));
        if (got.IsOk() && got->has_value() && std::stoi(**got) < floor) ++stale_reads;
      }
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  ShardSplitter splitter(directory_.get(), Base("admin"));
  const Status s = splitter.Split(Request("live", Key(30)));
  ASSERT_TRUE(s.IsOk()) << s.ToString();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop = true;
  writer.join();

  EXPECT_EQ(failures.load(), 0) << "a write failed outright during the split";
  EXPECT_EQ(stale_reads.load(), 0) << "a read went backwards across the split";
  for (int i = 0; i < kKeys; i += 3) {
    auto got = client_->Get(Key(i));
    ASSERT_TRUE(got.IsOk() && got->has_value()) << Key(i);
    EXPECT_EQ(std::stoi(**got), acked[i]) << "lost acknowledged write on " << Key(i);
  }
}

// A coordinator can die after any step. A new one must finish the job from the
// recorded progress and end in exactly the same place.
class ShardSplitResumeTest : public ShardSplitTest,
                             public ::testing::WithParamInterface<std::string> {};

TEST_P(ShardSplitResumeTest, ACrashedCoordinatorIsResumedByANewOne) {
  Start();
  for (int i = 0; i < 60; ++i) {
    const Status put = client_->Put(Key(i), "v" + std::to_string(i));
    ASSERT_TRUE(put.IsOk()) << Key(i) << ": " << put.ToString();
  }

  {
    ShardSplitter doomed(directory_.get(), Base("admin-a"));
    doomed.stop_after = [&](const std::string& step) { return step == GetParam(); };
    const Status s = doomed.Split(Request("crashy", Key(30)));
    ASSERT_FALSE(s.IsOk()) << "the coordinator should have stopped after " << GetParam();
  }

  ShardSplitter rescuer(directory_.get(), Base("admin-b"));
  const Status s = rescuer.Resume("crashy");
  ASSERT_TRUE(s.IsOk()) << "resume after " << GetParam() << ": " << s.ToString();

  for (int i = 0; i < 60; ++i) {
    auto got = client_->Get(Key(i));
    ASSERT_TRUE(got.IsOk()) << Key(i) << ": " << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << Key(i) << " missing after resuming from " << GetParam();
    EXPECT_EQ(**got, "v" + std::to_string(i));
  }
  // It can serve writes on both sides, and the data ended up where it belongs.
  ASSERT_TRUE(client_->Put(Key(5), "low").IsOk());
  ASSERT_TRUE(client_->Put(Key(55), "high").IsOk());
  WaitForQuiescence();
  EXPECT_FALSE(NodeHas({1, 2, 3}, Key(40)));
  EXPECT_FALSE(NodeHas({4, 5, 6}, Key(10)));

  // Running the same split again is a no-op, not a second split.
  EXPECT_TRUE(rescuer.Split(Request("crashy", Key(30))).IsOk());
  EXPECT_EQ(client_->ShardFor(Key(5)), 1U);
}

INSTANTIATE_TEST_SUITE_P(EveryStep, ShardSplitResumeTest,
                         ::testing::Values("fenced", "copied", "published", "released", "activated"));

TEST_F(ShardSplitTest, RefusesASplitKeyThatDoesNotBelongToTheNamedShard) {
  Start();
  ShardSplitter splitter(directory_.get(), Base("admin"));
  SplitRequest r = Request("bad", Key(10));
  r.source_shard = 99;
  EXPECT_FALSE(splitter.Split(r).IsOk());
  r = Request("bad2", "");
  EXPECT_FALSE(splitter.Split(r).IsOk());  // cannot split at the empty key
}

}  // namespace
}  // namespace raftkv::client
