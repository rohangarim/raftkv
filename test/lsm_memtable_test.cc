#include <string>

#include <gtest/gtest.h>

#include "lsm/memtable.h"
#include "lsm/write_batch.h"

namespace raftkv::lsm {
namespace {

TEST(MemTableTest, ReadsBackAValue) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "v");

  std::string value;
  const auto type = t.Get("k", kMaxSequenceNumber, &value);
  ASSERT_TRUE(type.has_value());
  EXPECT_EQ(*type, ValueType::kValue);
  EXPECT_EQ(value, "v");
}

TEST(MemTableTest, MissingKeyReturnsNullopt) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "v");
  std::string value;
  EXPECT_FALSE(t.Get("absent", kMaxSequenceNumber, &value).has_value());
}

TEST(MemTableTest, NewerSequenceShadowsOlder) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "old");
  t.Add(2, ValueType::kValue, "k", "new");

  std::string value;
  ASSERT_TRUE(t.Get("k", kMaxSequenceNumber, &value).has_value());
  EXPECT_EQ(value, "new");
}

// A snapshot must not see writes that happened after it was taken. This is
// the mechanism Phase 5's consistent snapshots are built on.
TEST(MemTableTest, SnapshotHidesLaterWrites) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "at-one");
  t.Add(5, ValueType::kValue, "k", "at-five");

  std::string value;
  ASSERT_TRUE(t.Get("k", 3, &value).has_value());
  EXPECT_EQ(value, "at-one");

  ASSERT_TRUE(t.Get("k", 5, &value).has_value());
  EXPECT_EQ(value, "at-five");
}

TEST(MemTableTest, ReadBeforeAnyWriteSeesNothing) {
  MemTable t;
  t.Add(10, ValueType::kValue, "k", "v");
  std::string value;
  EXPECT_FALSE(t.Get("k", 9, &value).has_value());
}

// A tombstone is a distinct outcome from "not present". The caller must stop
// searching older tables when it sees one, or a delete silently resurrects
// the value from a previous level.
TEST(MemTableTest, DeleteIsATombstoneNotAnAbsence) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "v");
  t.Add(2, ValueType::kDeletion, "k", "");

  std::string value;
  const auto type = t.Get("k", kMaxSequenceNumber, &value);
  ASSERT_TRUE(type.has_value()) << "tombstone must be reported, not hidden";
  EXPECT_EQ(*type, ValueType::kDeletion);
}

TEST(MemTableTest, SnapshotBeforeDeleteStillSeesTheValue) {
  MemTable t;
  t.Add(1, ValueType::kValue, "k", "v");
  t.Add(2, ValueType::kDeletion, "k", "");

  std::string value;
  const auto type = t.Get("k", 1, &value);
  ASSERT_TRUE(type.has_value());
  EXPECT_EQ(*type, ValueType::kValue);
  EXPECT_EQ(value, "v");
}

TEST(MemTableTest, DoesNotConfuseKeysThatArePrefixes) {
  MemTable t;
  t.Add(1, ValueType::kValue, "abc", "short");
  t.Add(2, ValueType::kValue, "abcd", "long");

  std::string value;
  ASSERT_TRUE(t.Get("abc", kMaxSequenceNumber, &value).has_value());
  EXPECT_EQ(value, "short");
  ASSERT_TRUE(t.Get("abcd", kMaxSequenceNumber, &value).has_value());
  EXPECT_EQ(value, "long");
}

TEST(MemTableTest, HandlesBinaryKeysAndValues) {
  const std::string key("a\0b", 3);
  const std::string val("\xff\x00\xfe", 3);
  MemTable t;
  t.Add(1, ValueType::kValue, key, val);

  std::string value;
  ASSERT_TRUE(t.Get(key, kMaxSequenceNumber, &value).has_value());
  EXPECT_EQ(value, val);
}

TEST(MemTableTest, MemoryUsageGrowsWithEntries) {
  MemTable t;
  EXPECT_EQ(t.ApproximateMemoryUsage(), 0U);
  t.Add(1, ValueType::kValue, "k", std::string(1000, 'x'));
  EXPECT_GT(t.ApproximateMemoryUsage(), 1000U);
}

// ---------------------------------------------------------------------------
// WriteBatch
// ---------------------------------------------------------------------------

class RecordingHandler : public WriteBatch::Handler {
 public:
  struct Entry {
    SequenceNumber seq;
    ValueType type;
    std::string key;
    std::string value;
  };

  void Put(SequenceNumber seq, std::string_view key, std::string_view value) override {
    entries.push_back({seq, ValueType::kValue, std::string(key), std::string(value)});
  }
  void Delete(SequenceNumber seq, std::string_view key) override {
    entries.push_back({seq, ValueType::kDeletion, std::string(key), ""});
  }

  std::vector<Entry> entries;
};

TEST(WriteBatchTest, StartsEmpty) {
  WriteBatch b;
  EXPECT_TRUE(b.Empty());
  EXPECT_EQ(b.Count(), 0U);
}

TEST(WriteBatchTest, CountsPutsAndDeletes) {
  WriteBatch b;
  b.Put("a", "1");
  b.Delete("b");
  b.Put("c", "3");
  EXPECT_EQ(b.Count(), 3U);
}

// Each record in a batch consumes its own sequence number, so an individual
// mutation inside a multi-key batch is still addressable by a snapshot.
TEST(WriteBatchTest, AssignsConsecutiveSequenceNumbers) {
  WriteBatch b;
  b.Put("a", "1");
  b.Delete("b");
  b.Put("c", "3");
  b.SetSequence(100);

  RecordingHandler h;
  ASSERT_TRUE(b.Iterate(&h).IsOk());
  ASSERT_EQ(h.entries.size(), 3U);
  EXPECT_EQ(h.entries[0].seq, 100U);
  EXPECT_EQ(h.entries[1].seq, 101U);
  EXPECT_EQ(h.entries[2].seq, 102U);
  EXPECT_EQ(h.entries[1].type, ValueType::kDeletion);
  EXPECT_EQ(h.entries[2].key, "c");
  EXPECT_EQ(h.entries[2].value, "3");
}

TEST(WriteBatchTest, SurvivesSerializationRoundTrip) {
  WriteBatch b;
  b.Put("k", std::string("v\0v", 3));
  b.Delete("gone");
  b.SetSequence(7);

  WriteBatch restored;
  ASSERT_TRUE(WriteBatch::FromContents(b.Contents(), &restored).IsOk());
  EXPECT_EQ(restored.Count(), 2U);
  EXPECT_EQ(restored.Sequence(), 7U);

  RecordingHandler h;
  ASSERT_TRUE(restored.Iterate(&h).IsOk());
  ASSERT_EQ(h.entries.size(), 2U);
  EXPECT_EQ(h.entries[0].value, std::string("v\0v", 3));
  EXPECT_EQ(h.entries[1].type, ValueType::kDeletion);
}

TEST(WriteBatchTest, ClearResetsCountAndContents) {
  WriteBatch b;
  b.Put("a", "1");
  b.Clear();
  EXPECT_EQ(b.Count(), 0U);
  EXPECT_TRUE(b.Empty());
}

// Deserialization must reject damage rather than surface it mid-recovery.
TEST(WriteBatchTest, RejectsATruncatedHeader) {
  WriteBatch out;
  EXPECT_EQ(WriteBatch::FromContents("short", &out).ErrCode(), Code::kCorruption);
}

TEST(WriteBatchTest, RejectsATruncatedRecord) {
  WriteBatch b;
  b.Put("key", "value");
  std::string bytes = b.Contents();
  bytes.resize(bytes.size() - 2);

  WriteBatch out;
  EXPECT_EQ(WriteBatch::FromContents(bytes, &out).ErrCode(), Code::kCorruption);
}

TEST(WriteBatchTest, RejectsACountThatDoesNotMatchTheRecords) {
  WriteBatch b;
  b.Put("a", "1");
  std::string bytes = b.Contents();
  bytes[8] = static_cast<char>(9);  // claim nine records

  WriteBatch out;
  EXPECT_EQ(WriteBatch::FromContents(bytes, &out).ErrCode(), Code::kCorruption);
}

TEST(WriteBatchTest, RejectsAnUnknownRecordType) {
  WriteBatch b;
  b.Put("a", "1");
  std::string bytes = b.Contents();
  bytes[12] = static_cast<char>(0x7E);  // type byte

  WriteBatch out;
  EXPECT_EQ(WriteBatch::FromContents(bytes, &out).ErrCode(), Code::kCorruption);
}

}  // namespace
}  // namespace raftkv::lsm
