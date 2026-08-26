// WAL durability and torn-write recovery.
//
// The tests that matter here are the damage ones. A WAL that round-trips
// cleanly is easy; a WAL that behaves correctly after the process died
// halfway through an append is the entire reason the file has CRCs in it.

#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lsm/wal.h"

namespace raftkv::lsm {
namespace {

// Distinguishes temp directories between tests in the same binary. Tests may
// run in parallel under ctest, so the pid alone is not enough.
int NextTempId() {
  static int counter = 0;
  return counter++;
}

class WalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_wal_test_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(dir_);
    path_ = dir_ / "wal.log";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  void WriteRecords(const std::vector<std::string>& records, bool sync = true) {
    auto opened = WalWriter::Open(path_);
    ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    auto writer = opened.TakeValue();
    for (const auto& r : records) {
      ASSERT_TRUE(writer->Append(r).IsOk());
    }
    if (sync) {
      ASSERT_TRUE(writer->Sync().IsOk());
    }
    ASSERT_TRUE(writer->Close().IsOk());
  }

  size_t FileSize() const { return std::filesystem::file_size(path_); }

  // Chops `bytes` off the end, simulating a crash partway through an append.
  void TruncateBy(size_t bytes) {
    const auto size = FileSize();
    std::filesystem::resize_file(path_, size - bytes);
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

TEST_F(WalTest, RoundTripsRecords) {
  const std::vector<std::string> records = {"alpha", "", "gamma with spaces",
                                            std::string("binary\0data", 11)};
  WriteRecords(records);

  std::vector<std::string> replayed;
  ASSERT_TRUE(ReplayWal(path_, /*truncate_partial=*/false, &replayed).IsOk());
  EXPECT_EQ(replayed, records);
}

TEST_F(WalTest, ReplayingAMissingFileIsNotAnError) {
  std::vector<std::string> replayed;
  const Status s = ReplayWal(dir_ / "does_not_exist.log", false, &replayed);
  EXPECT_TRUE(s.IsOk()) << s.ToString();
  EXPECT_TRUE(replayed.empty());
}

TEST_F(WalTest, RecoversRecordsBeforeATornTail) {
  WriteRecords({"first", "second", "third"});
  // Chop the middle of the last record: exactly what a crash mid-append does.
  TruncateBy(3);

  std::vector<std::string> replayed;
  ASSERT_TRUE(ReplayWal(path_, /*truncate_partial=*/true, &replayed).IsOk());
  EXPECT_EQ(replayed, (std::vector<std::string>{"first", "second"}));
}

TEST_F(WalTest, TruncationMakesTheFileAppendableAgain) {
  WriteRecords({"first", "second", "third"});
  TruncateBy(3);

  std::vector<std::string> replayed;
  ASSERT_TRUE(ReplayWal(path_, /*truncate_partial=*/true, &replayed).IsOk());
  ASSERT_EQ(replayed.size(), 2U);

  // After truncation the file must end on a record boundary, so new appends
  // land cleanly rather than after a hole.
  auto opened = WalWriter::Open(path_);
  ASSERT_TRUE(opened.IsOk());
  auto writer = opened.TakeValue();
  ASSERT_TRUE(writer->Append("fourth").IsOk());
  ASSERT_TRUE(writer->Sync().IsOk());
  ASSERT_TRUE(writer->Close().IsOk());

  std::vector<std::string> after;
  ASSERT_TRUE(ReplayWal(path_, false, &after).IsOk());
  EXPECT_EQ(after, (std::vector<std::string>{"first", "second", "fourth"}));
}

TEST_F(WalTest, DetectsAFlippedBitInThePayload) {
  WriteRecords({"first", "second"});
  CorruptByteAt(FileSize() - 1);

  std::vector<std::string> replayed;
  const Status s = ReplayWal(path_, /*truncate_partial=*/false, &replayed);
  EXPECT_EQ(s.ErrCode(), Code::kCorruption) << s.ToString();
  EXPECT_EQ(replayed, (std::vector<std::string>{"first"}))
      << "records before the damage must still be recovered";
}

// The CRC covers the length field, not just the payload. If it did not, a
// corrupted length would silently reframe every following record and their
// CRCs would still validate.
TEST_F(WalTest, DetectsACorruptedLengthField) {
  WriteRecords({"first", "second"});
  CorruptByteAt(4);  // length field of the first record

  std::vector<std::string> replayed;
  const Status s = ReplayWal(path_, false, &replayed);
  EXPECT_EQ(s.ErrCode(), Code::kCorruption) << s.ToString();
  EXPECT_TRUE(replayed.empty());
}

TEST_F(WalTest, DetectsAFlippedBitInTheStoredCrc) {
  WriteRecords({"first"});
  CorruptByteAt(0);

  std::vector<std::string> replayed;
  EXPECT_EQ(ReplayWal(path_, false, &replayed).ErrCode(), Code::kCorruption);
}

TEST_F(WalTest, HandlesAZeroLengthTail) {
  WriteRecords({"first"});
  // Leave only part of a header behind.
  std::filesystem::resize_file(path_, FileSize() + 0);
  const auto size = FileSize();
  std::filesystem::resize_file(path_, size + 3);  // 3 stray bytes

  std::vector<std::string> replayed;
  ASSERT_TRUE(ReplayWal(path_, /*truncate_partial=*/true, &replayed).IsOk());
  EXPECT_EQ(replayed, (std::vector<std::string>{"first"}));
  EXPECT_EQ(FileSize(), size) << "stray tail bytes must be trimmed";
}

TEST_F(WalTest, HandlesManyRecordsAcrossTheInternalBuffer) {
  std::vector<std::string> records;
  records.reserve(2000);
  for (int i = 0; i < 2000; ++i) {
    records.push_back("record-" + std::to_string(i) + std::string(100, 'p'));
  }
  WriteRecords(records);

  std::vector<std::string> replayed;
  ASSERT_TRUE(ReplayWal(path_, false, &replayed).IsOk());
  EXPECT_EQ(replayed, records);
}

}  // namespace
}  // namespace raftkv::lsm
