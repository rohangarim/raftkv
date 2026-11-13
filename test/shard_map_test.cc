#include "shard/shard_map.h"

#include <gtest/gtest.h>

namespace raftkv::shard {
namespace {

ShardRange Range(uint32_t id, std::string start) {
  return ShardRange{id, std::move(start), {"127.0.0.1:" + std::to_string(7000 + id)}};
}

ShardMap ThreeShards() {
  auto map = ShardMap::Create({Range(1, ""), Range(2, "g"), Range(3, "p")});
  EXPECT_TRUE(map.IsOk());
  return map.TakeValue();
}

TEST(ShardMapTest, LookupFindsTheOwningRange) {
  const ShardMap map = ThreeShards();
  EXPECT_EQ(map.Lookup("").shard_id, 1U);
  EXPECT_EQ(map.Lookup("apple").shard_id, 1U);
  EXPECT_EQ(map.Lookup("g").shard_id, 2U);  // a boundary belongs to the upper range
  EXPECT_EQ(map.Lookup("fzzz").shard_id, 1U);
  EXPECT_EQ(map.Lookup("moon").shard_id, 2U);
  EXPECT_EQ(map.Lookup("p").shard_id, 3U);
  EXPECT_EQ(map.Lookup("zebra").shard_id, 3U);
}

TEST(ShardMapTest, EveryKeyHasExactlyOneOwner) {
  const ShardMap map = ThreeShards();
  for (char c = 'a'; c <= 'z'; ++c) {
    const std::string key(1, c);
    int owners = 0;
    for (size_t i = 0; i < map.Ranges().size(); ++i) {
      const std::string end = map.EndOf(i);
      if (key >= map.Ranges()[i].start && (end.empty() || key < end)) {
        ++owners;
      }
    }
    EXPECT_EQ(owners, 1) << key;
  }
}

TEST(ShardMapTest, OverlappingReturnsOnlyTheRangesTheScanTouches) {
  const ShardMap map = ThreeShards();
  auto ids = [](const std::vector<const ShardRange*>& rs) {
    std::vector<uint32_t> out;
    out.reserve(rs.size());
    for (const auto* r : rs) {
      out.push_back(r->shard_id);
    }
    return out;
  };
  EXPECT_EQ(ids(map.Overlapping("a", "c")), (std::vector<uint32_t>{1}));
  EXPECT_EQ(ids(map.Overlapping("a", "g")), (std::vector<uint32_t>{1}));   // end is exclusive
  EXPECT_EQ(ids(map.Overlapping("a", "h")), (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(ids(map.Overlapping("h", "")), (std::vector<uint32_t>{2, 3}));
  EXPECT_EQ(ids(map.Overlapping("", "")), (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_TRUE(map.Overlapping("m", "m").empty());
  EXPECT_TRUE(map.Overlapping("z", "a").empty());
}

TEST(ShardMapTest, SplitCutsOneRangeAndBumpsTheEpoch) {
  const ShardMap map = ThreeShards();
  auto split = map.Split("k", Range(9, ""));
  ASSERT_TRUE(split.IsOk()) << split.GetStatus().ToString();
  EXPECT_EQ(split->Epoch(), map.Epoch() + 1);
  EXPECT_EQ(split->Ranges().size(), 4U);
  EXPECT_EQ(split->Lookup("h").shard_id, 2U);
  EXPECT_EQ(split->Lookup("k").shard_id, 9U);
  EXPECT_EQ(split->Lookup("o").shard_id, 9U);
  EXPECT_EQ(split->Lookup("p").shard_id, 3U);
  // The original is untouched.
  EXPECT_EQ(map.Lookup("k").shard_id, 2U);
}

TEST(ShardMapTest, SplitRejectsBoundariesAndTheEmptyKey) {
  const ShardMap map = ThreeShards();
  EXPECT_FALSE(map.Split("", Range(9, "")).IsOk());
  EXPECT_FALSE(map.Split("g", Range(9, "")).IsOk());
  EXPECT_FALSE(map.Split("k", Range(2, "")).IsOk());  // shard id already in use
}

TEST(ShardMapTest, CreateValidatesItsInput) {
  EXPECT_FALSE(ShardMap::Create({}).IsOk());
  EXPECT_FALSE(ShardMap::Create({Range(1, "a")}).IsOk());                   // must start at ""
  EXPECT_FALSE(ShardMap::Create({Range(1, ""), Range(2, "m"), Range(3, "c")}).IsOk());
  EXPECT_FALSE(ShardMap::Create({Range(1, ""), Range(2, "m"), Range(3, "m")}).IsOk());
  EXPECT_FALSE(ShardMap::Create({Range(1, ""), Range(1, "m")}).IsOk());
  ShardRange no_addr{1, "", {}};
  EXPECT_FALSE(ShardMap::Create({no_addr}).IsOk());
}

}  // namespace
}  // namespace raftkv::shard
