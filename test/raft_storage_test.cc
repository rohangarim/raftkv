// Durable Raft state.
//
// The interesting cases here are the damage ones. A log that round-trips
// cleanly is easy; a log that behaves correctly after the process died halfway
// through an append is why every record carries a CRC. Unlike the LSM WAL,
// nothing sits behind this log -- an entry acknowledged from here and then
// lost is a lost committed write -- so recovery has to be exactly right.

#include "storage/raft_storage.h"

#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace raftkv::storage {
namespace {

int NextTempId() {
  static int counter = 0;
  return counter++;
}

proto::Entry MakeEntry(uint64_t index, uint64_t term, const std::string& data = "") {
  proto::Entry e;
  e.set_index(index);
  e.set_term(term);
  e.set_type(proto::ENTRY_NORMAL);
  e.set_data(data);
  return e;
}

class RaftStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_storage_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::unique_ptr<RaftLogStore> OpenLog(uint64_t segment_bytes = uint64_t{16} * 1024 * 1024) {
    LogStoreOptions o;
    o.dir = dir_;
    o.segment_bytes = segment_bytes;
    auto opened = RaftLogStore::Open(std::move(o));
    EXPECT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    return opened.IsOk() ? opened.TakeValue() : nullptr;
  }

  std::vector<std::filesystem::path> Segments() const {
    std::vector<std::filesystem::path> out;
    for (const auto& e : std::filesystem::directory_iterator(dir_)) {
      if (e.path().extension() == ".log") {
        out.push_back(e.path());
      }
    }
    std::sort(out.begin(), out.end());
    return out;
  }

  void TruncateFileBy(const std::filesystem::path& p, size_t bytes) {
    const auto size = std::filesystem::file_size(p);
    std::filesystem::resize_file(p, size - bytes);
  }

  void CorruptByteAt(const std::filesystem::path& p, size_t offset) {
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(static_cast<std::streamoff>(offset));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0xFF);
    f.seekp(static_cast<std::streamoff>(offset));
    f.write(&c, 1);
  }

  std::filesystem::path dir_;
};

// ---------------------------------------------------------------------------
// HardStateStore
// ---------------------------------------------------------------------------

TEST_F(RaftStorageTest, HardStateStartsEmpty) {
  auto opened = HardStateStore::Open(dir_);
  ASSERT_TRUE(opened.IsOk());
  auto store = opened.TakeValue();
  EXPECT_EQ(store->Current().term(), 0U);
  EXPECT_EQ(store->Current().vote(), 0U);
}

TEST_F(RaftStorageTest, HardStateSurvivesReopen) {
  {
    auto store = HardStateStore::Open(dir_).TakeValue();
    proto::HardState hs;
    hs.set_term(7);
    hs.set_vote(3);
    hs.set_commit(42);
    ASSERT_TRUE(store->Save(hs).IsOk());
  }
  auto reopened = HardStateStore::Open(dir_);
  ASSERT_TRUE(reopened.IsOk());
  auto store = reopened.TakeValue();
  EXPECT_EQ(store->Current().term(), 7U);
  EXPECT_EQ(store->Current().vote(), 3U);
  EXPECT_EQ(store->Current().commit(), 42U);
}

TEST_F(RaftStorageTest, HardStateOverwritesRatherThanAppends) {
  auto store = HardStateStore::Open(dir_).TakeValue();
  for (uint64_t term = 1; term <= 5; ++term) {
    proto::HardState hs;
    hs.set_term(term);
    hs.set_vote(term);
    ASSERT_TRUE(store->Save(hs).IsOk());
  }
  auto reopened = HardStateStore::Open(dir_).TakeValue();
  EXPECT_EQ(reopened->Current().term(), 5U) << "a stale term survived a later write";
}

// A crash between creating the file and writing it leaves an empty file. That
// is a lost update, not corruption, and must not fail recovery.
TEST_F(RaftStorageTest, EmptyHardStateFileIsNotCorruption) {
  {
    std::ofstream f(dir_ / "HARDSTATE", std::ios::binary);
  }
  auto opened = HardStateStore::Open(dir_);
  ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
  EXPECT_EQ(opened.TakeValue()->Current().term(), 0U);
}

// The temp file must never be mistaken for the real one.
TEST_F(RaftStorageTest, LeftoverTempFileIsIgnored) {
  auto store = HardStateStore::Open(dir_).TakeValue();
  proto::HardState hs;
  hs.set_term(4);
  ASSERT_TRUE(store->Save(hs).IsOk());

  {
    std::ofstream f(dir_ / "HARDSTATE.tmp", std::ios::binary);
    f << "garbage";
  }

  auto reopened = HardStateStore::Open(dir_);
  ASSERT_TRUE(reopened.IsOk());
  EXPECT_EQ(reopened.TakeValue()->Current().term(), 4U);
}

// ---------------------------------------------------------------------------
// RaftLogStore
// ---------------------------------------------------------------------------

TEST_F(RaftStorageTest, LogStartsEmpty) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->LastIndex(), 0U);
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_TRUE(all->empty());
}

TEST_F(RaftStorageTest, AppendsAndReadsBack) {
  auto log = OpenLog();
  const std::vector<proto::Entry> entries = {MakeEntry(1, 1, "a"), MakeEntry(2, 1, "b"),
                                             MakeEntry(3, 2, "c")};
  ASSERT_TRUE(log->Append(entries).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  EXPECT_EQ(log->LastIndex(), 3U);

  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 3U);
  EXPECT_EQ((*all)[2].data(), "c");
  EXPECT_EQ((*all)[2].term(), 2U);
}

TEST_F(RaftStorageTest, RejectsAnAppendThatLeavesAHole) {
  auto log = OpenLog();
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1)}).IsOk());
  EXPECT_EQ(log->Append(std::vector<proto::Entry>{MakeEntry(3, 1)}).ErrCode(),
            lsm::Code::kInvalidArgument);
}

TEST_F(RaftStorageTest, SurvivesReopen) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(
        log->Append(std::vector<proto::Entry>{MakeEntry(1, 1, "x"), MakeEntry(2, 1, "y")}).IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->LastIndex(), 2U);
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 2U);
  EXPECT_EQ((*all)[1].data(), "y");
}

TEST_F(RaftStorageTest, AppendsContinueAfterReopen) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1)}).IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  auto log = OpenLog();
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(2, 1)}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_EQ(all->size(), 2U);
}

// A crash mid-append leaves a partial record. Everything before it is intact
// and was never acknowledged, so dropping it loses nothing promised.
TEST_F(RaftStorageTest, RecoversEntriesBeforeATornTail) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(
        log->Append(std::vector<proto::Entry>{MakeEntry(1, 1, "one"), MakeEntry(2, 1, "two"),
                                              MakeEntry(3, 1, "three")})
            .IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  TruncateFileBy(Segments().front(), 4);

  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 2U) << "a torn tail was accepted or too much was dropped";
  EXPECT_EQ((*all)[1].data(), "two");
  EXPECT_EQ(log->LastIndex(), 2U);
}

TEST_F(RaftStorageTest, RepairedLogAcceptsNewAppends) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1), MakeEntry(2, 1)}).IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  TruncateFileBy(Segments().front(), 3);

  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(log->LastIndex(), 1U);
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(2, 5, "new")}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());

  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 2U);
  EXPECT_EQ((*all)[1].term(), 5U);
}

TEST_F(RaftStorageTest, DetectsAFlippedBitInAnEntry) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1, "payload"),
                                                      MakeEntry(2, 1, "second")})
                    .IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  const auto size = std::filesystem::file_size(Segments().front());
  CorruptByteAt(Segments().front(), size - 2);

  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_EQ(all->size(), 1U) << "a corrupted record was accepted";
}

// The CRC covers the length field. Without that, a corrupted length silently
// reframes every following record and their CRCs still validate.
TEST_F(RaftStorageTest, DetectsACorruptedLengthField) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(
        log->Append(std::vector<proto::Entry>{MakeEntry(1, 1, "a"), MakeEntry(2, 1, "b")}).IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
  }
  CorruptByteAt(Segments().front(), 4);

  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_TRUE(all->empty()) << "a reframed record was accepted";
}

TEST_F(RaftStorageTest, RollsToNewSegmentsAndReadsAcrossThem) {
  auto log = OpenLog(/*segment_bytes=*/1024);
  std::vector<proto::Entry> entries;
  for (uint64_t i = 1; i <= 200; ++i) {
    entries.push_back(MakeEntry(i, 1, std::string(64, 'v')));
  }
  ASSERT_TRUE(log->Append(entries).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  EXPECT_GT(log->SegmentCount(), 1U) << "the log never rolled a segment";

  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 200U);
  EXPECT_EQ((*all)[199].index(), 200U);
}

// Damage in the middle of the log is not a crash artifact. Truncating there
// would silently discard every entry in the segments that follow.
TEST_F(RaftStorageTest, RejectsDamageInANonFinalSegment) {
  {
    auto log = OpenLog(/*segment_bytes=*/512);
    std::vector<proto::Entry> entries;
    for (uint64_t i = 1; i <= 100; ++i) {
      entries.push_back(MakeEntry(i, 1, std::string(64, 'v')));
    }
    ASSERT_TRUE(log->Append(entries).IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
    ASSERT_GT(log->SegmentCount(), 2U);
  }
  const auto segments = Segments();
  CorruptByteAt(segments.front(), std::filesystem::file_size(segments.front()) - 2);

  LogStoreOptions o;
  o.dir = dir_;
  o.segment_bytes = 512;
  auto opened = RaftLogStore::Open(std::move(o));
  EXPECT_FALSE(opened.IsOk()) << "damage in a non-final segment was silently truncated";
  if (!opened.IsOk()) {
    EXPECT_EQ(opened.GetStatus().ErrCode(), lsm::Code::kCorruption);
  }
}

TEST_F(RaftStorageTest, TruncateSuffixDropsDivergentEntries) {
  auto log = OpenLog();
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1), MakeEntry(2, 1),
                                                    MakeEntry(3, 1), MakeEntry(4, 1)})
                  .IsOk());
  ASSERT_TRUE(log->Sync().IsOk());

  ASSERT_TRUE(log->TruncateSuffix(3).IsOk());
  EXPECT_EQ(log->LastIndex(), 2U);

  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_EQ(all->size(), 2U);
}

// The truncation must itself be durable. If it is not, the discarded entries
// come back on restart -- the same class of bug the Phase 2 harness caught in
// the in-memory path (DECISIONS.md D-0029).
TEST_F(RaftStorageTest, TruncationSurvivesReopen) {
  {
    auto log = OpenLog();
    ASSERT_TRUE(
        log->Append(std::vector<proto::Entry>{MakeEntry(1, 1), MakeEntry(2, 1), MakeEntry(3, 1)})
            .IsOk());
    ASSERT_TRUE(log->Sync().IsOk());
    ASSERT_TRUE(log->TruncateSuffix(2).IsOk());
  }
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->LastIndex(), 1U) << "truncated entries came back after restart";
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  EXPECT_EQ(all->size(), 1U);
}

TEST_F(RaftStorageTest, TruncateThenAppendReplacesTheSuffix) {
  auto log = OpenLog();
  ASSERT_TRUE(
      log->Append(std::vector<proto::Entry>{MakeEntry(1, 1), MakeEntry(2, 1, "old")}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  ASSERT_TRUE(log->TruncateSuffix(2).IsOk());
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(2, 9, "new")}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());

  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_EQ(all->size(), 2U);
  EXPECT_EQ((*all)[1].data(), "new");
  EXPECT_EQ((*all)[1].term(), 9U);
}

TEST_F(RaftStorageTest, CompactPrefixRemovesWholeSegmentsOnly) {
  auto log = OpenLog(/*segment_bytes=*/512);
  std::vector<proto::Entry> entries;
  for (uint64_t i = 1; i <= 100; ++i) {
    entries.push_back(MakeEntry(i, 1, std::string(64, 'v')));
  }
  ASSERT_TRUE(log->Append(entries).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  const size_t before = log->SegmentCount();
  ASSERT_GT(before, 2U);

  ASSERT_TRUE(log->CompactPrefix(50).IsOk());
  EXPECT_LT(log->SegmentCount(), before) << "compaction freed nothing";

  // Everything above the compaction point must still be readable.
  auto all = log->ReadAll();
  ASSERT_TRUE(all.IsOk());
  ASSERT_FALSE(all->empty());
  EXPECT_EQ(all->back().index(), 100U);
  EXPECT_LE(all->front().index(), 51U);
}

TEST_F(RaftStorageTest, CompactPrefixNeverRemovesTheActiveSegment) {
  auto log = OpenLog();
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(1, 1), MakeEntry(2, 1)}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
  ASSERT_TRUE(log->CompactPrefix(99).IsOk());
  EXPECT_EQ(log->SegmentCount(), 1U);
  ASSERT_TRUE(log->Append(std::vector<proto::Entry>{MakeEntry(3, 1)}).IsOk());
  ASSERT_TRUE(log->Sync().IsOk());
}

}  // namespace
}  // namespace raftkv::storage
