#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lsm/internal_key.h"
#include "lsm/sstable.h"

namespace raftkv::lsm {
namespace {

int NextTempId() {
  static int counter = 0;
  return counter++;
}

class SsTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_sst_test_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(dir_);
    path_ = dir_ / "table.sst";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  struct Entry {
    std::string user_key;
    SequenceNumber seq;
    ValueType type;
    std::string value;
  };

  // Writes entries in the order given; they must already be sorted.
  void Build(const std::vector<Entry>& entries) {
    auto created = SsTableBuilder::Create(path_);
    ASSERT_TRUE(created.IsOk()) << created.GetStatus().ToString();
    auto builder = created.TakeValue();
    for (const auto& e : entries) {
      const std::string ik = MakeInternalKey(e.user_key, e.seq, e.type);
      ASSERT_TRUE(builder->Add(ik, e.value).IsOk());
    }
    ASSERT_TRUE(builder->Finish().IsOk());
  }

  std::shared_ptr<SsTable> OpenTable() {
    auto opened = SsTable::Open(path_);
    EXPECT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    return opened.IsOk() ? opened.TakeValue() : nullptr;
  }

  void CorruptByteAt(size_t offset) {
    std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(static_cast<std::streamoff>(offset));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0xFF);
    f.seekp(static_cast<std::streamoff>(offset));
    f.write(&c, 1);
  }

  std::filesystem::path dir_;
  std::filesystem::path path_;
};

TEST_F(SsTableTest, ReadsBackASingleKey) {
  Build({{"k", 1, ValueType::kValue, "v"}});
  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  auto got = table->Get("k", kMaxSequenceNumber, &value);
  ASSERT_TRUE(got.IsOk());
  ASSERT_TRUE(got->has_value());
  EXPECT_EQ(**got, ValueType::kValue);
  EXPECT_EQ(value, "v");
}

TEST_F(SsTableTest, MissingKeyReturnsNoValue) {
  Build({{"b", 1, ValueType::kValue, "v"}});
  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  for (const char* key : {"a", "c", "bb"}) {
    auto got = table->Get(key, kMaxSequenceNumber, &value);
    ASSERT_TRUE(got.IsOk());
    EXPECT_FALSE(got->has_value()) << "unexpected hit for " << key;
  }
}

TEST_F(SsTableTest, FindsEveryKeyAcrossManyIndexIntervals) {
  std::vector<Entry> entries;
  entries.reserve(500);
  for (int i = 0; i < 500; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "key%05d", i);
    entries.push_back({key, 1, ValueType::kValue, "value-" + std::to_string(i)});
  }
  Build(entries);

  auto table = OpenTable();
  ASSERT_NE(table, nullptr);
  EXPECT_GT(table->NumIndexEntries(), 1U) << "test must span multiple index anchors";

  std::string value;
  for (int i = 0; i < 500; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "key%05d", i);
    auto got = table->Get(key, kMaxSequenceNumber, &value);
    ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << "missing " << key;
    EXPECT_EQ(value, "value-" + std::to_string(i));
  }
}

// Keys that fall between two sparse-index anchors must still be found. Seeking
// to the anchor at-or-after the probe instead of the one before it skips them,
// and the bug only shows up for keys that are not themselves anchors.
TEST_F(SsTableTest, FindsKeysBetweenIndexAnchors) {
  std::vector<Entry> entries;
  for (int i = 0; i < 100; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "k%03d", i);
    entries.push_back({key, 1, ValueType::kValue, std::to_string(i)});
  }
  Build(entries);

  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  // kIndexInterval is 16, so these are deliberately off-anchor.
  for (int i : {1, 7, 15, 17, 31, 33, 99}) {
    char key[16];
    std::snprintf(key, sizeof(key), "k%03d", i);
    auto got = table->Get(key, kMaxSequenceNumber, &value);
    ASSERT_TRUE(got.IsOk());
    ASSERT_TRUE(got->has_value()) << "missing off-anchor key " << key;
    EXPECT_EQ(value, std::to_string(i));
  }
}

TEST_F(SsTableTest, SnapshotSelectsTheRightVersion) {
  // Sequence descending within a user key, which is the required order.
  Build({{"k", 9, ValueType::kValue, "at-nine"},
         {"k", 5, ValueType::kValue, "at-five"},
         {"k", 1, ValueType::kValue, "at-one"}});
  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  struct Case {
    SequenceNumber snapshot;
    const char* expected;
  };
  for (const auto& c : {Case{100, "at-nine"}, Case{9, "at-nine"}, Case{8, "at-five"},
                        Case{5, "at-five"}, Case{4, "at-one"}, Case{1, "at-one"}}) {
    auto got = table->Get("k", c.snapshot, &value);
    ASSERT_TRUE(got.IsOk());
    ASSERT_TRUE(got->has_value()) << "snapshot " << c.snapshot;
    EXPECT_EQ(value, c.expected) << "snapshot " << c.snapshot;
  }

  auto before = table->Get("k", 0, &value);
  ASSERT_TRUE(before.IsOk());
  EXPECT_FALSE(before->has_value()) << "nothing was written at or below sequence 0";
}

TEST_F(SsTableTest, TombstoneIsReportedDistinctlyFromAbsence) {
  Build({{"k", 2, ValueType::kDeletion, ""}, {"k", 1, ValueType::kValue, "v"}});
  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  auto got = table->Get("k", kMaxSequenceNumber, &value);
  ASSERT_TRUE(got.IsOk());
  ASSERT_TRUE(got->has_value());
  EXPECT_EQ(**got, ValueType::kDeletion);

  auto older = table->Get("k", 1, &value);
  ASSERT_TRUE(older.IsOk());
  ASSERT_TRUE(older->has_value());
  EXPECT_EQ(**older, ValueType::kValue);
  EXPECT_EQ(value, "v");
}

// A record larger than the scan chunk must not be dropped or truncated. A
// naive "read a fixed chunk and parse what fits" reader fails exactly here.
TEST_F(SsTableTest, HandlesValuesLargerThanTheScanChunk) {
  const std::string big(size_t{200} * 1024, 'z');
  Build({{"a", 1, ValueType::kValue, "small"},
         {"b", 1, ValueType::kValue, big},
         {"c", 1, ValueType::kValue, "after"}});

  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  auto got = table->Get("b", kMaxSequenceNumber, &value);
  ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
  ASSERT_TRUE(got->has_value());
  EXPECT_EQ(value.size(), big.size());
  EXPECT_EQ(value, big);

  // The record after the oversized one must still be reachable.
  auto after = table->Get("c", kMaxSequenceNumber, &value);
  ASSERT_TRUE(after.IsOk());
  ASSERT_TRUE(after->has_value());
  EXPECT_EQ(value, "after");
}

TEST_F(SsTableTest, IteratorVisitsEveryRecordInOrder) {
  std::vector<Entry> entries;
  for (int i = 0; i < 200; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "k%04d", i);
    entries.push_back({key, 1, ValueType::kValue, std::to_string(i)});
  }
  Build(entries);

  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  auto it = table->NewIterator();
  int seen = 0;
  std::string previous;
  const InternalKeyComparator cmp;
  while (true) {
    std::string_view key;
    std::string_view value;
    const Status s = it->Next(&key, &value);
    if (s.ErrCode() == Code::kNotFound) {
      break;
    }
    ASSERT_TRUE(s.IsOk()) << s.ToString();
    if (seen > 0) {
      EXPECT_TRUE(cmp(previous, key)) << "iterator emitted keys out of order";
    }
    previous.assign(key);
    EXPECT_EQ(value, std::to_string(seen));
    ++seen;
  }
  EXPECT_EQ(seen, 200);
}

TEST_F(SsTableTest, EmptyTableOpensAndYieldsNothing) {
  Build({});
  auto table = OpenTable();
  ASSERT_NE(table, nullptr);

  std::string value;
  auto got = table->Get("anything", kMaxSequenceNumber, &value);
  ASSERT_TRUE(got.IsOk());
  EXPECT_FALSE(got->has_value());

  auto it = table->NewIterator();
  std::string_view k;
  std::string_view v;
  EXPECT_EQ(it->Next(&k, &v).ErrCode(), Code::kNotFound);
}

TEST_F(SsTableTest, BuilderRejectsOutOfOrderKeys) {
  auto created = SsTableBuilder::Create(path_);
  ASSERT_TRUE(created.IsOk());
  auto builder = created.TakeValue();

  ASSERT_TRUE(builder->Add(MakeInternalKey("b", 1, ValueType::kValue), "1").IsOk());
  const Status s = builder->Add(MakeInternalKey("a", 1, ValueType::kValue), "2");
  EXPECT_EQ(s.ErrCode(), Code::kInvalidArgument) << "descending key must be rejected";

  // A repeat of the same key is not strictly ascending either.
  EXPECT_EQ(builder->Add(MakeInternalKey("b", 1, ValueType::kValue), "3").ErrCode(),
            Code::kInvalidArgument);
}

TEST_F(SsTableTest, RejectsAFileWithABadMagic) {
  Build({{"k", 1, ValueType::kValue, "v"}});
  const auto size = std::filesystem::file_size(path_);
  CorruptByteAt(size - 1);  // magic occupies the last four bytes

  auto opened = SsTable::Open(path_);
  EXPECT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), Code::kCorruption);
}

// The footer CRC exists so a corrupted index_offset is caught here rather than
// sending the reader off to parse arbitrary bytes as an index.
TEST_F(SsTableTest, RejectsACorruptedIndexOffset) {
  Build({{"k", 1, ValueType::kValue, "v"}});
  const auto size = std::filesystem::file_size(path_);
  CorruptByteAt(size - kFooterSize);  // first byte of index_offset

  auto opened = SsTable::Open(path_);
  EXPECT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), Code::kCorruption);
}

TEST_F(SsTableTest, RejectsATruncatedFile) {
  Build({{"k", 1, ValueType::kValue, "v"}});
  const auto size = std::filesystem::file_size(path_);
  std::filesystem::resize_file(path_, size / 2);

  auto opened = SsTable::Open(path_);
  EXPECT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), Code::kCorruption);
}

TEST_F(SsTableTest, RejectsAFileShorterThanTheFooter) {
  std::ofstream f(path_, std::ios::binary);
  f << "tiny";
  f.close();

  auto opened = SsTable::Open(path_);
  EXPECT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), Code::kCorruption);
}

}  // namespace
}  // namespace raftkv::lsm
