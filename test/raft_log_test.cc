#include "raft/raft_log.h"

#include <vector>

#include <gtest/gtest.h>

namespace raftkv::raft {
namespace {

proto::Entry MakeEntry(uint64_t index, uint64_t term, const std::string& data = "") {
  proto::Entry e;
  e.set_index(index);
  e.set_term(term);
  e.set_type(proto::ENTRY_NORMAL);
  e.set_data(data);
  return e;
}

std::vector<proto::Entry> Entries(std::initializer_list<std::pair<uint64_t, uint64_t>> pairs) {
  std::vector<proto::Entry> out;
  for (auto [index, term] : pairs) {
    out.push_back(MakeEntry(index, term));
  }
  return out;
}

TEST(RaftLogTest, StartsEmpty) {
  RaftLog log;
  EXPECT_EQ(log.LastIndex(), 0U);
  EXPECT_TRUE(log.Empty());
  auto t = log.TermAt(0);
  ASSERT_TRUE(t.IsOk());
  EXPECT_EQ(*t, 0U) << "index 0 means before the beginning and has term 0";
}

TEST(RaftLogTest, AppendsAndReadsBack) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 2}})).IsOk());
  EXPECT_EQ(log.LastIndex(), 3U);

  auto t = log.TermAt(3);
  ASSERT_TRUE(t.IsOk());
  EXPECT_EQ(*t, 2U);
}

TEST(RaftLogTest, RejectsAnAppendThatLeavesAHole) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}})).IsOk());
  EXPECT_EQ(log.Append(Entries({{3, 1}})).ErrCode(), lsm::Code::kInvalidArgument);
}

TEST(RaftLogTest, TermAtRejectsIndexPastTheEnd) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}})).IsOk());
  EXPECT_EQ(log.TermAt(2).GetStatus().ErrCode(), lsm::Code::kNotFound);
}

TEST(RaftLogTest, SliceClampsToWhatIsHeld) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 1}})).IsOk());
  EXPECT_EQ(log.Slice(2, 99).size(), 2U);
  EXPECT_EQ(log.Slice(5, 9).size(), 0U);
}

// Entries that already match must be left alone. Truncating on a match would
// discard entries the leader is about to resend -- and could discard a
// committed entry that arrived by another path.
TEST(RaftLogTest, MatchingEntriesAreNotTruncated) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 1}})).IsOk());

  ASSERT_TRUE(log.TruncateAndAppend(0, Entries({{1, 1}, {2, 1}})).IsOk());
  EXPECT_EQ(log.LastIndex(), 3U) << "a prefix that already matches must not shorten the log";
}

TEST(RaftLogTest, ConflictingEntryTruncatesTheTail) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 1}})).IsOk());

  // Index 2 arrives with a different term: it and everything after it go.
  ASSERT_TRUE(log.TruncateAndAppend(1, Entries({{2, 2}})).IsOk());
  EXPECT_EQ(log.LastIndex(), 2U);
  auto t = log.TermAt(2);
  ASSERT_TRUE(t.IsOk());
  EXPECT_EQ(*t, 2U);
}

TEST(RaftLogTest, AppendPastTheEndExtendsTheLog) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}})).IsOk());
  ASSERT_TRUE(log.TruncateAndAppend(1, Entries({{2, 1}, {3, 1}})).IsOk());
  EXPECT_EQ(log.LastIndex(), 3U);
}

TEST(RaftLogTest, RejectsAnAppendBeyondTheEnd) {
  RaftLog log;
  EXPECT_EQ(log.TruncateAndAppend(5, Entries({{6, 1}})).ErrCode(), lsm::Code::kInvalidArgument);
}

// The up-to-date check compares term first, then index. Reversing the order
// permits electing a leader that is missing committed entries.
TEST(RaftLogTest, UpToDateComparesTermBeforeIndex) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 2}})).IsOk());  // last: index 2, term 2

  EXPECT_TRUE(log.IsUpToDate(2, 2)) << "identical logs are up to date";
  EXPECT_TRUE(log.IsUpToDate(3, 1)) << "a higher last term wins even with a shorter log";
  EXPECT_FALSE(log.IsUpToDate(1, 99)) << "a longer log with a lower last term is NOT up to date";
  EXPECT_TRUE(log.IsUpToDate(2, 3)) << "same term, longer log";
  EXPECT_FALSE(log.IsUpToDate(2, 1)) << "same term, shorter log";
}

TEST(RaftLogTest, EmptyLogAcceptsAnyCandidate) {
  RaftLog log;
  EXPECT_TRUE(log.IsUpToDate(0, 0));
  EXPECT_TRUE(log.IsUpToDate(1, 1));
}

TEST(RaftLogTest, CompactionDropsAPrefixAndKeepsTheRest) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 2}, {4, 2}})).IsOk());

  log.CompactTo(2, 1);
  EXPECT_EQ(log.FirstIndex(), 3U);
  EXPECT_EQ(log.LastIndex(), 4U);
  EXPECT_EQ(log.SnapshotIndex(), 2U);

  auto boundary = log.TermAt(2);
  ASSERT_TRUE(boundary.IsOk()) << "the compaction boundary's term must remain answerable";
  EXPECT_EQ(*boundary, 1U);

  // A compacted index is unavailable, which is different from term 0. Reporting
  // 0 would make a stale AppendEntries appear to match.
  EXPECT_EQ(log.TermAt(1).GetStatus().ErrCode(), lsm::Code::kNotFound);
}

TEST(RaftLogTest, CompactingEverythingLeavesAnEmptyLogWithAKnownBoundary) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}})).IsOk());
  log.CompactTo(2, 1);
  EXPECT_TRUE(log.Empty());
  EXPECT_EQ(log.LastIndex(), 2U);
  auto t = log.TermAt(2);
  ASSERT_TRUE(t.IsOk());
  EXPECT_EQ(*t, 1U);
}

TEST(RaftLogTest, CompactingBackwardsIsIgnored) {
  RaftLog log;
  ASSERT_TRUE(log.Append(Entries({{1, 1}, {2, 1}, {3, 1}})).IsOk());
  log.CompactTo(2, 1);
  log.CompactTo(1, 1);
  EXPECT_EQ(log.SnapshotIndex(), 2U);
}

}  // namespace
}  // namespace raftkv::raft
