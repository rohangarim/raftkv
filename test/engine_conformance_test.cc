// One behavioural contract, every engine.
//
// Everything above src/engine/ is written once against engine::Engine, so the
// question "does the system still work on the other engine?" reduces to "do the
// engines agree on this contract?". Every case here runs against every engine
// built into this binary (the built-in LSM always, lsmtree when
// -DRAFTKV_WITH_LSMTREE=ON), under the name Engines/EngineConformanceTest.<case>/<engine>.
//
// A failure on one engine and not the other is a bug in that engine or its
// adapter, and the test name says which.

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include "engine/engine.h"

namespace raftkv::engine {
namespace {

std::string Key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
  return buf;
}

class EngineConformanceTest : public ::testing::TestWithParam<Kind> {
 protected:
  void SetUp() override {
    static int counter = 0;
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_engine_" + std::string(KindName(GetParam())) + "_" +
            std::to_string(::getpid()) + "_" + std::to_string(counter++));
    std::filesystem::remove_all(dir_);
  }

  void TearDown() override {
    engine_.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  // A small memtable by default, so ordinary-sized tests still exercise flushes
  // and compactions instead of living entirely in memory. Not smaller than 8 KiB:
  // lsmtree sizes its memtable in 4096-byte arena blocks and rotates once usage
  // exceeds the limit, so a limit of 4096 or less makes EVERY write flush a
  // one-entry memtable (thousands of tiny files and fsyncs, and a test that
  // appears to hang). That is a configuration pitfall in lsmtree, recorded in
  // DECISIONS.md, not something to exercise here.
  Options MakeOptions(size_t memtable_bytes = size_t{32} * 1024) const {
    Options o;
    o.dir = dir_;
    o.kind = GetParam();
    o.memtable_bytes = memtable_bytes;
    return o;
  }

  void OpenEngine(size_t memtable_bytes = size_t{32} * 1024) {
    engine_.reset();
    auto opened = Open(MakeOptions(memtable_bytes));
    ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    engine_ = opened.TakeValue();
  }

  std::optional<std::string> Get(const std::string& key) {
    auto got = engine_->Get(key);
    EXPECT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    return got.IsOk() ? *got : std::nullopt;
  }

  void Put(const std::string& key, const std::string& value) {
    Batch b;
    b.Put(key, value);
    ASSERT_TRUE(engine_->Write(b).IsOk());
  }

  void Del(const std::string& key) {
    Batch b;
    b.Delete(key);
    ASSERT_TRUE(engine_->Write(b).IsOk());
  }

  std::vector<std::pair<std::string, std::string>> ScanRange(const std::string& lo,
                                                             const std::string& hi,
                                                             size_t limit = 0) {
    std::vector<std::pair<std::string, std::string>> out;
    const Status s = engine_->Scan(lo, hi, limit, [&out](std::string_view k, std::string_view v) {
      out.emplace_back(std::string(k), std::string(v));
      return Status::Ok();
    });
    EXPECT_TRUE(s.IsOk()) << s.ToString();
    return out;
  }

  std::vector<std::pair<std::string, std::string>> ScanEverything() {
    std::vector<std::pair<std::string, std::string>> out;
    const Status s = engine_->ScanAll([&out](std::string_view k, std::string_view v) {
      out.emplace_back(std::string(k), std::string(v));
      return Status::Ok();
    });
    EXPECT_TRUE(s.IsOk()) << s.ToString();
    return out;
  }

  std::filesystem::path dir_;
  std::unique_ptr<Engine> engine_;
};

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, AFreshEngineIsEmpty) {
  OpenEngine();
  EXPECT_FALSE(Get("anything").has_value());
  EXPECT_TRUE(ScanRange("", "").empty());
  EXPECT_TRUE(ScanEverything().empty());
}

TEST_P(EngineConformanceTest, PutOverwriteAndDelete) {
  OpenEngine();
  Put("k", "one");
  EXPECT_EQ(Get("k"), "one");
  Put("k", "two");
  EXPECT_EQ(Get("k"), "two");
  Del("k");
  EXPECT_FALSE(Get("k").has_value());
  Put("k", "three");
  EXPECT_EQ(Get("k"), "three");
  Del("never-existed");  // deleting an absent key is not an error
  EXPECT_FALSE(Get("never-existed").has_value());
}

// An empty value is a value. Confusing it with "absent" is the classic way a
// state machine loses the distinction between "set to empty" and "never set".
TEST_P(EngineConformanceTest, AnEmptyValueIsPresentNotAbsent) {
  OpenEngine();
  Put("empty", "");
  auto got = Get("empty");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, "");
  EXPECT_EQ(ScanRange("", "").size(), 1U);
}

TEST_P(EngineConformanceTest, BinaryKeysAndValuesSurviveUntouched) {
  OpenEngine();
  const std::string key("a\0b\xff\x01", 5);
  const std::string value("\x00\x01\xfe\xff\x00", 5);
  Put(key, value);
  auto got = Get(key);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, value);
  const auto rows = ScanRange("", "");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0].first, key);
  EXPECT_EQ(rows[0].second, value);
}

TEST_P(EngineConformanceTest, KeysThatArePrefixesOfEachOtherStayDistinct) {
  OpenEngine();
  Put("ab", "short");
  Put("abc", "long");
  Put("abd", "other");
  EXPECT_EQ(Get("ab"), "short");
  EXPECT_EQ(Get("abc"), "long");
  const auto rows = ScanRange("", "");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0].first, "ab");
  EXPECT_EQ(rows[1].first, "abc");
  EXPECT_EQ(rows[2].first, "abd");
}

// ---------------------------------------------------------------------------
// Atomic batches
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, AnEmptyBatchIsANoOp) {
  OpenEngine();
  Batch empty;
  EXPECT_TRUE(engine_->Write(empty).IsOk());
  EXPECT_TRUE(ScanEverything().empty());
}

TEST_P(EngineConformanceTest, ABatchAppliesEveryOperation) {
  OpenEngine();
  Put("gone", "x");
  Batch b;
  b.Put("a", "1");
  b.Put("b", "2");
  b.Delete("gone");
  ASSERT_TRUE(engine_->Write(b).IsOk());
  EXPECT_EQ(Get("a"), "1");
  EXPECT_EQ(Get("b"), "2");
  EXPECT_FALSE(Get("gone").has_value());
}

// Within one batch the last operation on a key wins. The state machine relies on
// this: a transaction's commit deletes a lock and writes the value in one batch.
TEST_P(EngineConformanceTest, LaterOperationsInABatchWin) {
  OpenEngine();
  Batch b;
  b.Put("k", "first");
  b.Put("k", "second");
  b.Put("d", "x");
  b.Delete("d");
  b.Delete("r");
  b.Put("r", "revived");
  ASSERT_TRUE(engine_->Write(b).IsOk());
  EXPECT_EQ(Get("k"), "second");
  EXPECT_FALSE(Get("d").has_value());
  EXPECT_EQ(Get("r"), "revived");
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, ScanHonoursBoundsLimitAndOrder) {
  OpenEngine();
  for (int i = 0; i < 50; ++i) {
    Put(Key(i), "v" + std::to_string(i));
  }

  auto all = ScanRange("", "");
  ASSERT_EQ(all.size(), 50U);
  for (int i = 1; i < 50; ++i) {
    EXPECT_LT(all[static_cast<size_t>(i) - 1].first, all[static_cast<size_t>(i)].first);
  }

  // [lo, hi): lo inclusive, hi exclusive.
  auto mid = ScanRange(Key(10), Key(20));
  ASSERT_EQ(mid.size(), 10U);
  EXPECT_EQ(mid.front().first, Key(10));
  EXPECT_EQ(mid.back().first, Key(19));

  // An empty hi is unbounded; an empty lo starts at the beginning.
  EXPECT_EQ(ScanRange(Key(45), "").size(), 5U);
  EXPECT_EQ(ScanRange("", Key(5)).size(), 5U);

  // limit counts rows, and 0 means no limit.
  EXPECT_EQ(ScanRange("", "", 7).size(), 7U);
  EXPECT_EQ(ScanRange(Key(10), Key(20), 3).size(), 3U);
  EXPECT_EQ(ScanRange("", "", 0).size(), 50U);

  // A range that matches nothing, and one that is inverted, are both empty.
  EXPECT_TRUE(ScanRange(Key(100), Key(200)).empty());
  EXPECT_TRUE(ScanRange(Key(20), Key(10)).empty());
}

TEST_P(EngineConformanceTest, ScanSkipsDeletedAndSeesOnlyTheNewestVersion) {
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);  // force the data onto disk
  for (int i = 0; i < 300; ++i) {
    Put(Key(i), "old" + std::to_string(i) + std::string(40, 'x'));
  }
  for (int i = 0; i < 300; i += 2) {
    Del(Key(i));
  }
  for (int i = 0; i < 300; i += 3) {
    Put(Key(i), "new" + std::to_string(i));
  }

  const auto rows = ScanRange("", "");
  std::map<std::string, std::string> expect;
  for (int i = 0; i < 300; ++i) {
    if (i % 3 == 0) {
      expect[Key(i)] = "new" + std::to_string(i);
    } else if (i % 2 != 0) {
      expect[Key(i)] = "old" + std::to_string(i) + std::string(40, 'x');
    }
  }
  ASSERT_EQ(rows.size(), expect.size());
  size_t idx = 0;
  for (const auto& [k, v] : expect) {
    EXPECT_EQ(rows[idx].first, k);
    EXPECT_EQ(rows[idx].second, v) << "stale version surfaced for " << k;
    ++idx;
  }
}

// What the callback returns must reach the caller unchanged, and stops the scan.
// The state machine uses this to abort a scan on the first conflicting lock.
TEST_P(EngineConformanceTest, ACallbackErrorStopsTheScanAndPropagates) {
  OpenEngine();
  for (int i = 0; i < 20; ++i) {
    Put(Key(i), "v");
  }
  int seen = 0;
  const Status s = engine_->Scan("", "", 0, [&seen](std::string_view, std::string_view) {
    ++seen;
    return seen == 5 ? Status::InvalidArgument("stop here") : Status::Ok();
  });
  EXPECT_EQ(seen, 5) << "the scan kept going after the callback failed";
  EXPECT_EQ(s.ErrCode(), lsm::Code::kInvalidArgument);
  EXPECT_NE(s.ToString().find("stop here"), std::string::npos) << s.ToString();

  const Status all = engine_->ScanAll(
      [](std::string_view, std::string_view) { return Status::Corruption("bad row"); });
  EXPECT_EQ(all.ErrCode(), lsm::Code::kCorruption);
}

// Keys compare as UNSIGNED bytes. A signed-char comparison would put 0x80 and
// 0xff before 0x00 and 'a', and two replicas on different engines would then
// disagree about scan order and about which keys a range split hands over.
TEST_P(EngineConformanceTest, KeysSortAsUnsignedBytes) {
  OpenEngine();
  const std::string nul("\0", 1);
  for (const std::string& k :
       {std::string("\xff"), std::string("a"), std::string("\x80"), nul, std::string("\x7f")}) {
    Put(k, "v");
  }
  const auto rows = ScanRange("", "");
  ASSERT_EQ(rows.size(), 5U);
  const std::vector<std::string> want = {nul, "a", "\x7f", "\x80", "\xff"};
  for (size_t i = 0; i < want.size(); ++i) {
    EXPECT_EQ(rows[i].first, want[i]) << "position " << i;
  }
  // And a bounded scan across the sign boundary agrees.
  EXPECT_EQ(ScanRange("\x7f", "\xff").size(), 2U);
}

// ---------------------------------------------------------------------------
// ScanAll: the consistent image a Raft snapshot is built from
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, ScanAllReturnsTheWholeLiveSetInOrder) {
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 500; ++i) {
    model[Key(i)] = "value" + std::to_string(i);
    Put(Key(i), model[Key(i)]);
  }
  for (int i = 0; i < 500; i += 5) {
    model.erase(Key(i));
    Del(Key(i));
  }
  const auto rows = ScanEverything();
  ASSERT_EQ(rows.size(), model.size());
  size_t idx = 0;
  for (const auto& [k, v] : model) {
    EXPECT_EQ(rows[idx].first, k);
    EXPECT_EQ(rows[idx].second, v);
    ++idx;
  }
}

// A snapshot must be an image of ONE moment. Writes that land while it is being
// read -- new keys, overwrites, deletes -- must not leak into it. The writer is
// started from inside the scan and is deliberately not joined there: an engine
// that blocks writers during a scan would deadlock if it were, and whether it
// blocks is a performance property this contract does not constrain.
TEST_P(EngineConformanceTest, ScanAllIsAConsistentImageDespiteConcurrentWrites) {
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);
  std::map<std::string, std::string> before;
  for (int i = 0; i < 400; ++i) {
    before[Key(i)] = "before" + std::to_string(i);
    Put(Key(i), before[Key(i)]);
  }

  std::thread writer;
  std::atomic<bool> writer_done{false};
  std::vector<std::pair<std::string, std::string>> rows;
  const Status s = engine_->ScanAll([&](std::string_view k, std::string_view v) {
    if (!writer.joinable()) {
      writer = std::thread([&] {
        for (int i = 0; i < 400; ++i) {
          Batch b;
          b.Put(Key(i), "after" + std::to_string(i));  // overwrite everything
          b.Put(Key(1000 + i), "brand-new");           // and add keys
          if (i % 7 == 0) {
            b.Delete(Key(i));
          }
          (void)engine_->Write(b);
        }
        writer_done.store(true);
      });
    }
    rows.emplace_back(std::string(k), std::string(v));
    // Give the writer a real chance to interleave with the scan.
    if (rows.size() % 50 == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return Status::Ok();
  });
  if (writer.joinable()) {
    writer.join();
  }
  ASSERT_TRUE(s.IsOk()) << s.ToString();

  ASSERT_EQ(rows.size(), before.size()) << "the image gained or lost rows";
  size_t idx = 0;
  for (const auto& [k, v] : before) {
    EXPECT_EQ(rows[idx].first, k);
    EXPECT_EQ(rows[idx].second, v) << "a write made during the scan leaked into it";
    ++idx;
  }
  // And the writes did land afterwards.
  EXPECT_EQ(Get(Key(1)), "after1");
  EXPECT_EQ(Get(Key(1001)), "brand-new");
  EXPECT_FALSE(Get(Key(7)).has_value());
}

// Read atomicity: a batch is visible to a reader entirely or not at all.
//
// This is a different guarantee from crash atomicity (all or none after a crash),
// and it is the one transactions rely on: a linearizable Scan that sees half of a
// multi-key commit, or a Raft snapshot that does, is not explainable by any
// sequential history. Two writers' worth of pressure is not needed to expose a
// violation; one writer applying two-key batches in a loop while readers create
// scans is enough, because the window is the writer's whole apply.
TEST_P(EngineConformanceTest, AReaderNeverSeesHalfOfABatch) {
  OpenEngine(/*memtable_bytes=*/size_t{64} * 1024);
  Put("pair-a", "0");
  Put("pair-b", "0");

  std::atomic<bool> stop{false};
  std::atomic<int> torn{0};
  std::atomic<long> reads{0};
  std::string first_torn;
  std::mutex note_mutex;

  std::thread writer([&] {
    for (int i = 1; !stop.load(); ++i) {
      Batch b;
      b.Put("pair-a", std::to_string(i));
      b.Put("pair-b", std::to_string(i));
      if (!engine_->Write(b).IsOk()) {
        ++torn;
        return;
      }
      // A real writer is a Raft apply loop, paced by the log's fsync. Yielding
      // between batches keeps this from degenerating into a lock-hogging spin
      // that no real workload resembles (and that a loaded machine turns into
      // reader starvation rather than a test of read atomicity).
      std::this_thread::yield();
    }
  });

  auto check = [&](const std::vector<std::pair<std::string, std::string>>& rows, const char* how) {
    ++reads;
    if (rows.size() != 2 || rows[0].second != rows[1].second) {
      if (torn.fetch_add(1) == 0) {
        const std::lock_guard<std::mutex> lock(note_mutex);
        first_torn = std::string(how) + ": saw " +
                     (rows.empty() ? "nothing" : rows[0].first + "=" + rows[0].second) +
                     (rows.size() > 1 ? " and " + rows[1].first + "=" + rows[1].second : "");
      }
    }
  };
  std::vector<std::thread> readers;
  readers.reserve(2);
  for (int t = 0; t < 2; ++t) {
    readers.emplace_back([&, t] {
      while (!stop.load()) {
        std::vector<std::pair<std::string, std::string>> rows;
        if (t == 0) {
          (void)engine_->Scan("pair-", "pair.", 0, [&rows](std::string_view k, std::string_view v) {
            rows.emplace_back(std::string(k), std::string(v));
            return Status::Ok();
          });
          check(rows, "Scan");
        } else {
          (void)engine_->ScanAll([&rows](std::string_view k, std::string_view v) {
            rows.emplace_back(std::string(k), std::string(v));
            return Status::Ok();
          });
          check(rows, "ScanAll");
        }
      }
    });
  }
  // Run for at least 1.5 s AND until the readers have done a meaningful number
  // of reads (capped), so a loaded machine lengthens the test instead of making
  // it inconclusive.
  const auto began = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - began < std::chrono::milliseconds(1500) ||
         (reads.load() < 20000L &&
          std::chrono::steady_clock::now() - began < std::chrono::seconds(20))) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  stop.store(true);
  writer.join();
  for (auto& r : readers) {
    r.join();
  }
  EXPECT_GT(reads.load(), 1000L) << "the readers barely ran, so this proved nothing";
  EXPECT_EQ(torn.load(), 0) << "a reader saw part of a batch: " << first_torn << " (after "
                            << reads.load() << " reads)";
}

// A scan that is slow, or whose callback blocks, must not stop writers. A Raft
// snapshot is exactly such a scan: it reads the whole database while the node
// keeps applying entries. The callback here blocks until a writer has completed
// fifty writes; if the engine (or an adapter holding a lock across iteration)
// stalled writers for the length of the scan, the writer could never finish and
// the wait would time out.
TEST_P(EngineConformanceTest, WritersAreNotBlockedByAScanInProgress) {
  OpenEngine(/*memtable_bytes=*/size_t{64} * 1024);
  for (int i = 0; i < 50; ++i) {
    Put(Key(i), "v");
  }

  std::atomic<int> written{0};
  std::thread writer;
  bool writer_finished_in_time = false;
  const Status s = engine_->ScanAll([&](std::string_view, std::string_view) {
    if (!writer.joinable()) {
      writer = std::thread([&] {
        for (int i = 0; i < 50; ++i) {
          Batch b;
          b.Put("during-scan-" + std::to_string(i), "x");
          if (engine_->Write(b).IsOk()) {
            ++written;
          }
        }
      });
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (written.load() < 50 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      writer_finished_in_time = written.load() >= 50;
    }
    return Status::Ok();
  });
  if (writer.joinable()) {
    writer.join();
  }
  ASSERT_TRUE(s.IsOk()) << s.ToString();
  EXPECT_TRUE(writer_finished_in_time)
      << "writers were blocked while a scan was in progress (" << written.load() << "/50 done)";
}

// Reads must not starve a writer. An exclusive lock taken by writers behaves
// differently across standard libraries -- a reader-preferring rwlock lets a
// steady stream of readers keep a writer out indefinitely -- so this is checked
// rather than assumed.
TEST_P(EngineConformanceTest, AWriterMakesProgressWhileReadersHammer) {
  OpenEngine();
  Put("hot", "v");
  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  readers.reserve(4);
  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&] {
      while (!stop.load()) {
        (void)engine_->Get("hot");
      }
    });
  }
  const auto start = std::chrono::steady_clock::now();
  int done = 0;
  for (; done < 200 && std::chrono::steady_clock::now() - start < std::chrono::seconds(10);
       ++done) {
    Batch b;
    b.Put("w" + std::to_string(done), "x");
    ASSERT_TRUE(engine_->Write(b).IsOk());
  }
  stop.store(true);
  for (auto& r : readers) {
    r.join();
  }
  EXPECT_EQ(done, 200) << "the writer was starved by readers";
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, ReopenRecoversEverythingIncludingDeletes) {
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 600; ++i) {
    model[Key(i)] = "v" + std::to_string(i) + std::string(30, 'p');
    Put(Key(i), model[Key(i)]);
  }
  for (int i = 0; i < 600; i += 4) {
    model.erase(Key(i));
    Del(Key(i));
  }
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);  // closes the first, reopens the directory

  const auto rows = ScanEverything();
  ASSERT_EQ(rows.size(), model.size());
  size_t idx = 0;
  for (const auto& [k, v] : model) {
    EXPECT_EQ(rows[idx].first, k);
    EXPECT_EQ(rows[idx].second, v);
    ++idx;
  }
  EXPECT_FALSE(Get(Key(0)).has_value()) << "a deleted key came back after reopen";
}

// A worker process writes, takes a durability barrier, writes more, and is then
// killed with SIGKILL at a moment it did not choose. After recovery, everything
// before the barrier must be present, and what was written after it must be an
// unbroken prefix: each write is one atomic batch, so a hole would mean a batch
// was torn.
//
// This is process-crash safety, which a kernel-held page cache provides even
// without fsync. It says nothing about power loss, which cannot be tested from
// inside a process; see README "What it doesn't do yet".
TEST_P(EngineConformanceTest, WritesBeforeABarrierSurviveSigkill) {
  constexpr int kSynced = 300;
  constexpr int kUnsynced = 300;

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto opened = Open(MakeOptions());
    if (!opened.IsOk()) {
      ::_exit(2);
    }
    auto child = opened.TakeValue();
    for (int i = 0; i < kSynced + kUnsynced; ++i) {
      Batch b;
      b.Put(Key(i), "v" + std::to_string(i));
      if (!child->Write(b).IsOk()) {
        ::_exit(3);
      }
      if (i == kSynced - 1 && !child->SyncWal().IsOk()) {
        ::_exit(4);
      }
    }
    ::raise(SIGKILL);
    ::_exit(5);
  }

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFSIGNALED(status))
      << "the worker exited normally with status " << WEXITSTATUS(status);
  ASSERT_EQ(WTERMSIG(status), SIGKILL);

  OpenEngine();
  for (int i = 0; i < kSynced; ++i) {
    auto got = Get(Key(i));
    ASSERT_TRUE(got.has_value()) << "write " << i << " was before the barrier and was lost";
    EXPECT_EQ(*got, "v" + std::to_string(i));
  }
  int recovered_prefix = 0;
  for (int i = kSynced; i < kSynced + kUnsynced; ++i) {
    if (!Get(Key(i)).has_value()) {
      break;
    }
    ++recovered_prefix;
  }
  for (int i = kSynced + recovered_prefix; i < kSynced + kUnsynced; ++i) {
    EXPECT_FALSE(Get(Key(i)).has_value())
        << "key " << i << " survived but an earlier unsynced key did not: a hole in the log";
  }
}

// ---------------------------------------------------------------------------
// DestroyContents: snapshot restore replaces state, it does not merge into it
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, DestroyContentsEmptiesAndRemainsUsable) {
  OpenEngine();
  for (int i = 0; i < 200; ++i) {
    Put(Key(i), "old");
  }
  ASSERT_TRUE(engine_->DestroyContents().IsOk());
  EXPECT_TRUE(ScanEverything().empty());
  EXPECT_FALSE(Get(Key(5)).has_value());

  Put("fresh", "data");
  EXPECT_EQ(Get("fresh"), "data");
}

// The failure this guards: a destroy that leaves a log or manifest behind, so the
// NEXT open resurrects the data the restore was supposed to replace.
TEST_P(EngineConformanceTest, DestroyedDataDoesNotComeBackAfterReopen) {
  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);
  for (int i = 0; i < 300; ++i) {
    Put(Key(i), "old" + std::string(50, 'z'));
  }
  ASSERT_TRUE(engine_->DestroyContents().IsOk());
  Put("survivor", "yes");

  OpenEngine(/*memtable_bytes=*/size_t{8} * 1024);
  const auto rows = ScanEverything();
  ASSERT_EQ(rows.size(), 1U) << "old data was resurrected by reopening after a destroy";
  EXPECT_EQ(rows[0].first, "survivor");
}

// Readers run on gRPC handler threads while a snapshot restore runs elsewhere.
// A reader inside the old database while it is torn down is a use-after-free,
// which only the sanitizers can see -- so this must run under them.
TEST_P(EngineConformanceTest, ReadersDuringDestroyContentsNeitherCrashNorSeeGarbage) {
  OpenEngine();
  for (int i = 0; i < 100; ++i) {
    Put(Key(i), "v");
  }
  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::vector<std::thread> readers;
  readers.reserve(3);
  for (int t = 0; t < 3; ++t) {
    readers.emplace_back([&] {
      while (!stop.load()) {
        auto got = engine_->Get(Key(7));
        if (!got.IsOk() || (got->has_value() && **got != "v")) {
          ++bad;
        }
        (void)engine_->Scan("", "", 5,
                            [](std::string_view, std::string_view) { return Status::Ok(); });
      }
    });
  }
  for (int round = 0; round < 6; ++round) {
    ASSERT_TRUE(engine_->DestroyContents().IsOk());
    for (int i = 0; i < 100; ++i) {
      Put(Key(i), "v");
    }
  }
  stop.store(true);
  for (auto& r : readers) {
    r.join();
  }
  EXPECT_EQ(bad.load(), 0);
}

// ---------------------------------------------------------------------------
// Model-based: random operations against std::map
// ---------------------------------------------------------------------------

TEST_P(EngineConformanceTest, RandomOperationsAgreeWithAReferenceMap) {
  for (uint64_t seed = 1; seed <= 4; ++seed) {
    std::filesystem::remove_all(dir_);
    OpenEngine(/*memtable_bytes=*/size_t{16} * 1024);
    std::mt19937_64 rng(seed);
    std::map<std::string, std::string> model;
    std::uniform_int_distribution<int> key_space(0, 250);
    std::uniform_int_distribution<int> action(0, 99);
    std::uniform_int_distribution<int> value_len(0, 300);

    auto random_value = [&] {
      return std::string(static_cast<size_t>(value_len(rng)),
                         static_cast<char>('a' + (rng() % 26)));
    };

    for (int step = 0; step < 4000; ++step) {
      const int a = action(rng);
      if (a < 40) {
        const std::string k = Key(key_space(rng));
        const std::string v = random_value();
        Put(k, v);
        model[k] = v;
      } else if (a < 55) {
        const std::string k = Key(key_space(rng));
        Del(k);
        model.erase(k);
      } else if (a < 70) {
        Batch b;
        const int n = 2 + static_cast<int>(rng() % 4);
        for (int j = 0; j < n; ++j) {
          const std::string k = Key(key_space(rng));
          if (rng() % 3 == 0) {
            b.Delete(k);
            model.erase(k);
          } else {
            const std::string v = random_value();
            b.Put(k, v);
            model[k] = v;
          }
        }
        ASSERT_TRUE(engine_->Write(b).IsOk());
      } else if (a < 90) {
        const std::string k = Key(key_space(rng));
        auto it = model.find(k);
        auto got = Get(k);
        if (it == model.end()) {
          ASSERT_FALSE(got.has_value()) << "seed " << seed << " step " << step << " key " << k;
        } else {
          ASSERT_TRUE(got.has_value()) << "seed " << seed << " step " << step << " key " << k;
          ASSERT_EQ(*got, it->second) << "seed " << seed << " step " << step << " key " << k;
        }
      } else if (a < 98) {
        int lo = key_space(rng);
        int hi = key_space(rng);
        if (lo > hi) {
          std::swap(lo, hi);
        }
        const size_t limit = (rng() % 3 == 0) ? static_cast<size_t>(1 + rng() % 8) : 0;
        const auto rows = ScanRange(Key(lo), Key(hi), limit);
        std::vector<std::pair<std::string, std::string>> expect;
        for (auto it = model.lower_bound(Key(lo)); it != model.end() && it->first < Key(hi); ++it) {
          expect.emplace_back(it->first, it->second);
          if (limit != 0 && expect.size() == limit) {
            break;
          }
        }
        ASSERT_EQ(rows, expect) << "seed " << seed << " step " << step << " scan [" << lo << ","
                                << hi << ") limit " << limit;
      } else if (a == 98) {
        ASSERT_TRUE(engine_->SyncWal().IsOk());
      } else {
        OpenEngine(/*memtable_bytes=*/size_t{16} * 1024);  // close and reopen mid-run
      }
    }

    const auto rows = ScanEverything();
    ASSERT_EQ(rows.size(), model.size()) << "seed " << seed;
    size_t idx = 0;
    for (const auto& [k, v] : model) {
      ASSERT_EQ(rows[idx].first, k) << "seed " << seed;
      ASSERT_EQ(rows[idx].second, v) << "seed " << seed;
      ++idx;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(Engines, EngineConformanceTest, ::testing::ValuesIn(AvailableKinds()),
                         [](const ::testing::TestParamInfo<Kind>& info) {
                           return std::string(KindName(info.param));
                         });

// ---------------------------------------------------------------------------
// Selecting an engine
// ---------------------------------------------------------------------------

TEST(EngineKindTest, NamesRoundTrip) {
  for (Kind kind : {Kind::kBuiltin, Kind::kLsmTree}) {
    auto parsed = ParseKind(KindName(kind));
    ASSERT_TRUE(parsed.IsOk());
    EXPECT_EQ(*parsed, kind);
  }
  EXPECT_TRUE(ParseKind("own").IsOk()) << "'own' is accepted as an alias";
  EXPECT_FALSE(ParseKind("rocksdb").IsOk());
}

TEST(EngineKindTest, TheBuiltinEngineIsAlwaysAvailable) {
  EXPECT_TRUE(Available(Kind::kBuiltin));
  const auto kinds = AvailableKinds();
  ASSERT_FALSE(kinds.empty());
  EXPECT_EQ(kinds.front(), Kind::kBuiltin);
}

TEST(EngineKindTest, OpeningAnUnavailableEngineFailsLoudly) {
  if (Available(Kind::kLsmTree)) {
    GTEST_SKIP() << "lsmtree is built in";
  }
  Options o;
  o.dir = std::filesystem::temp_directory_path() / "raftkv_engine_unavailable";
  o.kind = Kind::kLsmTree;
  auto opened = Open(o);
  ASSERT_FALSE(opened.IsOk());
  EXPECT_EQ(opened.GetStatus().ErrCode(), lsm::Code::kNotSupported);
}

}  // namespace
}  // namespace raftkv::engine
