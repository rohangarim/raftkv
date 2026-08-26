#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lsm/db.h"

namespace raftkv::lsm {
namespace {

int NextTempId() {
  static int counter = 0;
  return counter++;
}

class DbTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_db_test_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    db_.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  Options MakeOptions() const {
    Options o;
    o.dir = dir_;
    // Large by default so tests control flushing explicitly rather than
    // tripping it by accident.
    o.memtable_bytes = size_t{64} * 1024 * 1024;
    return o;
  }

  void OpenDb(Options options) {
    db_.reset();
    auto opened = DB::Open(std::move(options));
    ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    db_ = opened.TakeValue();
  }

  void OpenDb() { OpenDb(MakeOptions()); }

  std::optional<std::string> Get(std::string_view key, const Snapshot* snap = nullptr) {
    auto got = db_->Get(key, snap);
    EXPECT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    return got.IsOk() ? got.TakeValue() : std::nullopt;
  }

  std::filesystem::path WalPath() const { return dir_ / "LOG.wal"; }

  std::filesystem::path dir_;
  std::unique_ptr<DB> db_;
};

TEST_F(DbTest, PutThenGet) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "v").IsOk());
  EXPECT_EQ(Get("k"), "v");
}

TEST_F(DbTest, MissingKeyReturnsNullopt) {
  OpenDb();
  EXPECT_FALSE(Get("absent").has_value());
}

TEST_F(DbTest, OverwriteReturnsTheNewestValue) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "first").IsOk());
  ASSERT_TRUE(db_->Put("k", "second").IsOk());
  EXPECT_EQ(Get("k"), "second");
}

TEST_F(DbTest, DeleteHidesTheValue) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "v").IsOk());
  ASSERT_TRUE(db_->Delete("k").IsOk());
  EXPECT_FALSE(Get("k").has_value());
}

TEST_F(DbTest, PutAfterDeleteRestoresTheKey) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "v1").IsOk());
  ASSERT_TRUE(db_->Delete("k").IsOk());
  ASSERT_TRUE(db_->Put("k", "v2").IsOk());
  EXPECT_EQ(Get("k"), "v2");
}

TEST_F(DbTest, HandlesEmptyValuesAndBinaryKeys) {
  OpenDb();
  const std::string key("a\0b", 3);
  ASSERT_TRUE(db_->Put(key, "").IsOk());
  auto got = Get(key);
  ASSERT_TRUE(got.has_value()) << "an empty value is present, not absent";
  EXPECT_EQ(*got, "");
}

TEST_F(DbTest, WritesAWholeBatch) {
  OpenDb();
  WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("a");
  ASSERT_TRUE(db_->Write(batch).IsOk());

  EXPECT_FALSE(Get("a").has_value());
  EXPECT_EQ(Get("b"), "2");
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

TEST_F(DbTest, RecoversWrittenKeysAfterReopen) {
  OpenDb();
  ASSERT_TRUE(db_->Put("a", "1").IsOk());
  ASSERT_TRUE(db_->Put("b", "2").IsOk());
  ASSERT_TRUE(db_->Delete("a").IsOk());
  ASSERT_TRUE(db_->SyncWal().IsOk());

  OpenDb();
  EXPECT_FALSE(Get("a").has_value()) << "a delete must survive recovery";
  EXPECT_EQ(Get("b"), "2");
}

TEST_F(DbTest, RecoversFromSsTablesAfterFlush) {
  OpenDb();
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(db_->Put("key" + std::to_string(i), "value" + std::to_string(i)).IsOk());
  }
  ASSERT_TRUE(db_->FlushMemTable().IsOk());
  ASSERT_EQ(db_->NumL0Tables(), 1U);

  OpenDb();
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i)), "value" + std::to_string(i));
  }
}

TEST_F(DbTest, RecoversAcrossBothTablesAndTheWal) {
  OpenDb();
  ASSERT_TRUE(db_->Put("flushed", "old").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());
  ASSERT_TRUE(db_->Put("unflushed", "new").IsOk());
  ASSERT_TRUE(db_->SyncWal().IsOk());

  OpenDb();
  EXPECT_EQ(Get("flushed"), "old");
  EXPECT_EQ(Get("unflushed"), "new");
}

TEST_F(DbTest, SequenceNumbersResumeAfterRecovery) {
  OpenDb();
  ASSERT_TRUE(db_->Put("a", "1").IsOk());
  ASSERT_TRUE(db_->Put("b", "2").IsOk());
  const SequenceNumber before = db_->LastSequence();
  ASSERT_GT(before, 0U);
  ASSERT_TRUE(db_->SyncWal().IsOk());

  OpenDb();
  EXPECT_EQ(db_->LastSequence(), before)
      << "resuming below the old sequence would let new writes be shadowed by old ones";
}

// ---------------------------------------------------------------------------
// The atomicity guarantee the Raft state machine depends on.
// ---------------------------------------------------------------------------

// A command's effect and the applied index that records it live in the same
// batch, therefore in the same WAL record. A crash partway through the append
// must roll BOTH back, never one without the other. A surviving index without
// its effect silently drops a committed write; a surviving effect without its
// index causes the command to be applied twice on replay.
TEST_F(DbTest, EffectAndAppliedIndexRollBackTogether) {
  static constexpr const char* kAppliedIndex = "\x01applied_index";

  OpenDb();
  // Two complete, durable batches.
  for (int i = 1; i <= 2; ++i) {
    WriteBatch batch;
    batch.Put("user_key", "value" + std::to_string(i));
    batch.Put(kAppliedIndex, std::to_string(i));
    ASSERT_TRUE(db_->Write(batch).IsOk());
  }
  ASSERT_TRUE(db_->SyncWal().IsOk());

  // A third batch that reaches the disk only partially, which is exactly what
  // a crash mid-append leaves behind.
  {
    WriteBatch batch;
    batch.Put("user_key", "value3");
    batch.Put(kAppliedIndex, "3");
    ASSERT_TRUE(db_->Write(batch).IsOk());
    ASSERT_TRUE(db_->SyncWal().IsOk());
  }
  db_.reset();

  const auto size = std::filesystem::file_size(WalPath());
  std::filesystem::resize_file(WalPath(), size - 4);

  OpenDb();
  EXPECT_EQ(Get("user_key"), "value2") << "the torn batch's effect must not survive";
  EXPECT_EQ(Get(kAppliedIndex), "2") << "the applied index must roll back with it";
}

TEST_F(DbTest, RecoveryLeavesTheLogAppendable) {
  OpenDb();
  ASSERT_TRUE(db_->Put("a", "1").IsOk());
  ASSERT_TRUE(db_->SyncWal().IsOk());
  db_.reset();

  const auto size = std::filesystem::file_size(WalPath());
  std::filesystem::resize_file(WalPath(), size + 5);  // stray tail bytes

  OpenDb();
  EXPECT_EQ(Get("a"), "1");
  ASSERT_TRUE(db_->Put("b", "2").IsOk()) << "writes must resume after a repaired log";
  ASSERT_TRUE(db_->SyncWal().IsOk());

  OpenDb();
  EXPECT_EQ(Get("a"), "1");
  EXPECT_EQ(Get("b"), "2");
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

TEST_F(DbTest, SnapshotDoesNotSeeLaterWrites) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "before").IsOk());
  auto snap = db_->TakeSnapshot();
  ASSERT_TRUE(db_->Put("k", "after").IsOk());

  EXPECT_EQ(Get("k"), "after");
  EXPECT_EQ(Get("k", snap.get()), "before");
}

TEST_F(DbTest, SnapshotDoesNotSeeLaterDeletes) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "v").IsOk());
  auto snap = db_->TakeSnapshot();
  ASSERT_TRUE(db_->Delete("k").IsOk());

  EXPECT_FALSE(Get("k").has_value());
  EXPECT_EQ(Get("k", snap.get()), "v");
}

TEST_F(DbTest, SnapshotDoesNotSeeKeysCreatedAfterIt) {
  OpenDb();
  auto snap = db_->TakeSnapshot();
  ASSERT_TRUE(db_->Put("later", "v").IsOk());
  EXPECT_FALSE(Get("later", snap.get()).has_value());
}

TEST_F(DbTest, SnapshotSurvivesAFlush) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "before").IsOk());
  auto snap = db_->TakeSnapshot();
  ASSERT_TRUE(db_->Put("k", "after").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());

  EXPECT_EQ(Get("k", snap.get()), "before");
  EXPECT_EQ(Get("k"), "after");
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

TEST_F(DbTest, CompactionPreservesEveryLiveKey) {
  OpenDb();
  for (int round = 0; round < 4; ++round) {
    for (int i = 0; i < 25; ++i) {
      ASSERT_TRUE(db_->Put("key" + std::to_string(i),
                           "round" + std::to_string(round) + "-" + std::to_string(i))
                      .IsOk());
    }
    ASSERT_TRUE(db_->FlushMemTable().IsOk());
  }
  ASSERT_EQ(db_->NumL0Tables(), 4U);

  ASSERT_TRUE(db_->CompactRange().IsOk());
  EXPECT_EQ(db_->NumL0Tables(), 0U);
  EXPECT_TRUE(db_->HasL1Table());

  for (int i = 0; i < 25; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i)), "round3-" + std::to_string(i))
        << "newest version must win after compaction";
  }
}

// A tombstone that is dropped during compaction resurrects the value from the
// older table it was shadowing. This is the classic LSM delete bug.
TEST_F(DbTest, DeletedKeysStayDeletedAcrossCompaction) {
  OpenDb();
  ASSERT_TRUE(db_->Put("doomed", "v").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());
  ASSERT_TRUE(db_->Delete("doomed").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());

  ASSERT_TRUE(db_->CompactRange().IsOk());
  EXPECT_FALSE(Get("doomed").has_value()) << "compaction resurrected a deleted key";

  OpenDb();
  EXPECT_FALSE(Get("doomed").has_value()) << "and it came back after reopen";
}

TEST_F(DbTest, CompactedDataSurvivesReopen) {
  OpenDb();
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 20; ++i) {
      ASSERT_TRUE(db_->Put("k" + std::to_string(i), "r" + std::to_string(round)).IsOk());
    }
    ASSERT_TRUE(db_->FlushMemTable().IsOk());
  }
  ASSERT_TRUE(db_->CompactRange().IsOk());

  OpenDb();
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(Get("k" + std::to_string(i)), "r2");
  }
}

// A snapshot holds shared_ptrs to the tables it was taken over, so compaction
// must not delete those files while it lives.
TEST_F(DbTest, CompactionDoesNotBreakALiveSnapshot) {
  OpenDb();
  ASSERT_TRUE(db_->Put("k", "before").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());
  auto snap = db_->TakeSnapshot();

  ASSERT_TRUE(db_->Put("k", "after").IsOk());
  ASSERT_TRUE(db_->FlushMemTable().IsOk());
  ASSERT_TRUE(db_->CompactRange().IsOk());

  EXPECT_EQ(Get("k", snap.get()), "before")
      << "snapshot read failed after its table was compacted away";
  EXPECT_EQ(Get("k"), "after");
}

TEST_F(DbTest, FlushTriggersAutomaticallyOnSize) {
  Options o = MakeOptions();
  o.memtable_bytes = 4096;
  o.l0_compaction_trigger = 1000;  // isolate the flush trigger
  OpenDb(std::move(o));

  for (int i = 0; i < 200; ++i) {
    ASSERT_TRUE(db_->Put("key" + std::to_string(i), std::string(100, 'v')).IsOk());
  }
  EXPECT_GT(db_->NumL0Tables(), 0U) << "size trigger never fired";

  for (int i = 0; i < 200; ++i) {
    EXPECT_TRUE(Get("key" + std::to_string(i)).has_value()) << "lost key" << i;
  }
}

TEST_F(DbTest, CompactionTriggersAutomatically) {
  Options o = MakeOptions();
  o.memtable_bytes = 2048;
  o.l0_compaction_trigger = 2;
  OpenDb(std::move(o));

  for (int i = 0; i < 400; ++i) {
    ASSERT_TRUE(db_->Put("key" + std::to_string(i), std::string(100, 'v')).IsOk());
  }
  EXPECT_TRUE(db_->HasL1Table()) << "compaction trigger never fired";

  for (int i = 0; i < 400; ++i) {
    EXPECT_TRUE(Get("key" + std::to_string(i)).has_value()) << "lost key" << i;
  }
}

TEST_F(DbTest, SyncOnWriteMakesDataDurableWithoutAnExplicitSync) {
  Options o = MakeOptions();
  o.sync_on_write = true;  // the Phase 8 baseline configuration
  OpenDb(std::move(o));

  ASSERT_TRUE(db_->Put("k", "v").IsOk());
  db_.reset();  // no SyncWal() call

  OpenDb();
  EXPECT_EQ(Get("k"), "v");
}

TEST_F(DbTest, OpeningAFreshDirectoryYieldsAnEmptyDatabase) {
  OpenDb();
  EXPECT_EQ(db_->LastSequence(), 0U);
  EXPECT_EQ(db_->NumL0Tables(), 0U);
  EXPECT_FALSE(db_->HasL1Table());
}

TEST_F(DbTest, RejectsAnEmptyDirectoryOption) {
  Options o;
  auto opened = DB::Open(std::move(o));
  EXPECT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv::lsm
