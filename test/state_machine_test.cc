// State machine adapter: determinism, exactly-once, and snapshot fidelity.
//
// The tests that matter here are the dedup and atomicity ones. Everything else
// is plumbing; those two are what the Phase 10 linearizability checker will
// punish us for getting wrong, and it will do so in a way that looks like a
// consensus bug rather than a state machine bug.

#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kv.pb.h"

#include "statemachine/kv_state_machine.h"

namespace raftkv::statemachine {
namespace {

int NextTempId() {
  static int counter = 0;
  return counter++;
}

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::string EncodePut(const std::string& client, uint64_t seq, const std::string& key,
                      const std::string& value) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  c.mutable_put()->set_key(key);
  c.mutable_put()->set_value(value);
  return c.SerializeAsString();
}

std::string EncodeDelete(const std::string& client, uint64_t seq, const std::string& key) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  c.mutable_delete_()->set_key(key);
  return c.SerializeAsString();
}

std::string EncodeCas(const std::string& client, uint64_t seq, const std::string& key,
                      const std::optional<std::string>& expected, const std::string& new_value) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* cas = c.mutable_cas();
  cas->set_key(key);
  if (expected.has_value()) {
    cas->set_expected(*expected);
  }
  cas->set_new_value(new_value);
  return c.SerializeAsString();
}

struct BatchSpec {
  std::vector<std::pair<std::string, std::optional<std::string>>> checks;  // key, expected
  std::vector<std::pair<std::string, std::optional<std::string>>>
      writes;  // key, value (nullopt = delete)
};

std::string EncodeBatch(const std::string& client, uint64_t seq, const BatchSpec& spec) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* batch = c.mutable_batch();
  for (const auto& [key, expected] : spec.checks) {
    auto* check = batch->add_checks();
    check->set_key(key);
    if (expected.has_value()) {
      check->set_expected(*expected);
    }
  }
  for (const auto& [key, value] : spec.writes) {
    auto* op = batch->add_ops();
    if (value.has_value()) {
      op->mutable_put()->set_key(key);
      op->mutable_put()->set_value(*value);
    } else {
      op->mutable_delete_()->set_key(key);
    }
  }
  return c.SerializeAsString();
}

std::string EncodeRange(const std::string& client, uint64_t seq, proto::RangeCmd::Kind kind,
                        const std::string& lo, const std::string& hi, bool unbounded,
                        uint64_t epoch, bool empty = false) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* r = c.mutable_range();
  r->set_kind(kind);
  r->set_lo(lo);
  r->set_hi(hi);
  r->set_unbounded_hi(unbounded);
  r->set_empty(empty);
  r->set_epoch(epoch);
  return c.SerializeAsString();
}

std::string EncodeImport(const std::string& client, uint64_t seq,
                         const std::vector<std::pair<std::string, std::string>>& pairs) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  for (const auto& [k, v] : pairs) {
    auto* p = c.mutable_import()->add_puts();
    p->set_key(k);
    p->set_value(v);
  }
  return c.SerializeAsString();
}

struct TxnKeySpec {
  TxnKeySpec(std::string k, proto::LockRecord::Kind kd, std::string v, bool validates = false,
             bool absent = false, std::string expects = "")
      : key(std::move(k)),
        kind(kd),
        value(std::move(v)),
        validate(validates),
        expected_absent(absent),
        expected(std::move(expects)) {}
  std::string key;
  proto::LockRecord::Kind kind;
  std::string value;
  bool validate;
  bool expected_absent;
  std::string expected;
};

std::string EncodePrewrite(const std::string& client, uint64_t seq, const std::string& txid,
                           const std::string& primary, const std::vector<TxnKeySpec>& keys) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* t = c.mutable_txn();
  t->set_kind(proto::TxnCmd::PREWRITE);
  t->set_txid(txid);
  t->set_primary_key(primary);
  for (const auto& k : keys) {
    auto* w = t->add_writes();
    w->set_key(k.key);
    w->set_kind(k.kind);
    w->set_value(k.value);
    w->set_validate(k.validate);
    w->set_expected_absent(k.expected_absent);
    w->set_expected(k.expected);
  }
  return c.SerializeAsString();
}

std::string EncodeTxnEnd(const std::string& client, uint64_t seq, proto::TxnCmd::Kind kind,
                         const std::string& txid, const std::vector<std::string>& keys,
                         bool primary = false) {
  proto::Command c;
  c.set_client_id(client);
  c.set_seq(seq);
  auto* t = c.mutable_txn();
  t->set_kind(kind);
  t->set_txid(txid);
  t->set_primary(primary);
  for (const auto& k : keys) t->add_keys(k);
  return c.SerializeAsString();
}

// Parameterized over the storage engine: the state machine's behaviour -- dedup,
// transactions, range ownership, snapshots -- is specified once and must hold on
// every engine (see test/engine_conformance_test.cc for the engine contract).
class StateMachineTest : public ::testing::TestWithParam<engine::Kind> {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("raftkv_sm_test_" + std::to_string(::getpid()) + "_" + std::to_string(NextTempId()));
    std::filesystem::create_directories(dir_);
    Open();
  }

  void TearDown() override {
    sm_.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  void Open(const std::filesystem::path* dir = nullptr) {
    sm_.reset();
    engine::Options o;
    o.dir = dir != nullptr ? *dir : dir_;
    o.kind = GetParam();
    o.memtable_bytes = size_t{64} * 1024 * 1024;
    auto opened = KvStateMachine::Open(o);
    ASSERT_TRUE(opened.IsOk()) << opened.GetStatus().ToString();
    sm_ = opened.TakeValue();
  }

  ApplyResult Apply(uint64_t index, const std::string& encoded) {
    return sm_->Apply(index, AsBytes(encoded));
  }

  std::optional<std::string> Get(const std::string& key) {
    auto got = sm_->Get(key);
    EXPECT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    return got.IsOk() ? got.TakeValue() : std::nullopt;
  }

  std::filesystem::path dir_;
  std::unique_ptr<KvStateMachine> sm_;
};

TEST_P(StateMachineTest, AppliesAPut) {
  const auto r = Apply(1, EncodePut("c1", 1, "k", "v"));
  ASSERT_TRUE(r.status.IsOk()) << r.status.ToString();
  EXPECT_FALSE(r.deduplicated);
  EXPECT_EQ(Get("k"), "v");
  EXPECT_EQ(sm_->LastAppliedIndex(), 1U);
}

TEST_P(StateMachineTest, AppliesADelete) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodeDelete("c1", 2, "k")).status.IsOk());
  EXPECT_FALSE(Get("k").has_value());
}

TEST_P(StateMachineTest, AdvancesTheAppliedIndex) {
  for (uint64_t i = 1; i <= 5; ++i) {
    ASSERT_TRUE(Apply(i, EncodePut("c1", i, "k", std::to_string(i))).status.IsOk());
    EXPECT_EQ(sm_->LastAppliedIndex(), i);
  }
}

// ---------------------------------------------------------------------------
// Exactly-once
// ---------------------------------------------------------------------------

// This is the test the whole session table exists for. A client that retries
// after a leader failover resends the same (client_id, seq); applying it twice
// is a linearizability violation the Phase 10 checker will find.
TEST_P(StateMachineTest, ReplayingASequenceAppliesOnlyOnce) {
  const std::string cmd = EncodeCas("c1", 1, "counter", std::nullopt, "created");

  const auto first = Apply(1, cmd);
  ASSERT_TRUE(first.status.IsOk());
  EXPECT_FALSE(first.deduplicated);
  EXPECT_FALSE(first.cas_mismatch);

  // Same command, different log index: the retry the client sent after a
  // failover, which the new leader committed at a fresh position.
  const auto second = Apply(2, cmd);
  ASSERT_TRUE(second.status.IsOk());
  EXPECT_TRUE(second.deduplicated) << "the retry was applied a second time";
  EXPECT_FALSE(second.cas_mismatch)
      << "a re-applied create would have reported a mismatch, proving double application";

  EXPECT_EQ(Get("counter"), "created");
}

TEST_P(StateMachineTest, DedupReturnsTheOriginalCachedValue) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "original")).status.IsOk());
  // A different client changes the key in between.
  ASSERT_TRUE(Apply(2, EncodePut("c2", 1, "k", "changed")).status.IsOk());

  const auto retry = Apply(3, EncodePut("c1", 1, "k", "original"));
  EXPECT_TRUE(retry.deduplicated);
  EXPECT_EQ(Get("k"), "changed") << "the retry must not have re-applied its write";
}

TEST_P(StateMachineTest, DifferentClientsDoNotShareASequenceSpace) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "a", "1")).status.IsOk());
  const auto other = Apply(2, EncodePut("c2", 1, "b", "2"));
  EXPECT_FALSE(other.deduplicated) << "c2 seq 1 is unrelated to c1 seq 1";
  EXPECT_EQ(Get("b"), "2");
}

TEST_P(StateMachineTest, AnOlderSequenceIsRejectedRatherThanReapplied) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v1")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c1", 2, "k", "v2")).status.IsOk());

  // Only the most recent result is cached, so seq 1 can no longer be answered.
  // Re-applying it would silently roll the key back to v1.
  const auto stale = Apply(3, EncodePut("c1", 1, "k", "v1"));
  EXPECT_FALSE(stale.status.IsOk()) << "a stale sequence must be an explicit error";
  EXPECT_EQ(Get("k"), "v2");
}

TEST_P(StateMachineTest, DedupSurvivesRestart) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 7, "k", "v")).status.IsOk());
  ASSERT_TRUE(sm_->Sync().IsOk());

  Open();
  const auto retry = Apply(2, EncodePut("c1", 7, "k", "v"));
  EXPECT_TRUE(retry.deduplicated) << "the session table did not survive recovery";
}

// A client that restarts with the same client_id and a sequence counter reset to
// 1 sends DIFFERENT commands under sequence numbers the session has already
// seen. Treating those as retries means answering OK for writes that were never
// applied -- silent data loss, with no error anywhere. A true retry is
// byte-identical, so a reused sequence number carrying different bytes is a
// client bug that must fail loudly.
TEST_P(StateMachineTest, ReusingASequenceNumberForADifferentCommandIsRejected) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "first", "a")).status.IsOk());

  const auto reused = Apply(2, EncodePut("c1", 1, "second", "b"));
  EXPECT_FALSE(reused.status.IsOk())
      << "a different write under an already-used sequence number was acknowledged";
  EXPECT_EQ(reused.status.ErrCode(), lsm::Code::kInvalidArgument);
  EXPECT_FALSE(reused.deduplicated);
  EXPECT_FALSE(Get("second").has_value()) << "and it must not have been applied either";
  EXPECT_EQ(Get("first"), "a");
}

TEST_P(StateMachineTest, AnIdenticalRetryIsStillAnsweredFromTheSession) {
  const std::string cmd = EncodePut("c1", 1, "k", "v");
  ASSERT_TRUE(Apply(1, cmd).status.IsOk());
  const auto retry = Apply(2, cmd);
  EXPECT_TRUE(retry.status.IsOk());
  EXPECT_TRUE(retry.deduplicated);
}

// The session digest has to survive a restart, or the protection disappears
// exactly when a restarting client needs it.
TEST_P(StateMachineTest, TheReuseCheckSurvivesARestart) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "first", "a")).status.IsOk());
  ASSERT_TRUE(sm_->Sync().IsOk());
  Open();
  const auto reused = Apply(2, EncodePut("c1", 1, "second", "b"));
  EXPECT_FALSE(reused.status.IsOk());
}

// A command answered from the session cache is still a log entry that was
// consumed. If the state machine's applied index did not move, it would trail
// Raft's, and a snapshot taken at the Raft index would be refused.
TEST_P(StateMachineTest, ADeduplicatedCommandStillAdvancesTheAppliedIndex) {
  const std::string cmd = EncodePut("c1", 1, "k", "v");
  ASSERT_TRUE(Apply(1, cmd).status.IsOk());
  ASSERT_TRUE(Apply(2, cmd).deduplicated);
  EXPECT_EQ(sm_->LastAppliedIndex(), 2U);

  const auto stale = Apply(4, EncodePut("c1", 2, "k", "w"));
  ASSERT_TRUE(stale.status.IsOk());
  const auto old = Apply(5, cmd);  // seq 1 again, now older than the cached seq 2
  EXPECT_FALSE(old.status.IsOk());
  EXPECT_EQ(sm_->LastAppliedIndex(), 5U) << "a rejected command left the applied index behind";
}

TEST_P(StateMachineTest, CommandsWithoutAClientIdAreNotDeduplicated) {
  // Internal commands (a leader's no-op entry, for instance) carry no client.
  const auto first = Apply(1, EncodePut("", 0, "k", "v1"));
  ASSERT_TRUE(first.status.IsOk());
  const auto second = Apply(2, EncodePut("", 0, "k", "v2"));
  EXPECT_FALSE(second.deduplicated);
  EXPECT_EQ(Get("k"), "v2");
}

// Recovery replays the tail of the Raft log. An index at or below the applied
// index must be ignored, or every restart re-applies its last commands.
TEST_P(StateMachineTest, ReplayingAnOldIndexIsIgnored) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v1")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c2", 1, "k", "v2")).status.IsOk());

  const auto replay = Apply(1, EncodePut("c3", 1, "k", "should-not-apply"));
  EXPECT_TRUE(replay.deduplicated);
  EXPECT_EQ(Get("k"), "v2");
  EXPECT_EQ(sm_->LastAppliedIndex(), 2U);
}

// ---------------------------------------------------------------------------
// CAS
// ---------------------------------------------------------------------------

TEST_P(StateMachineTest, CasCreatesOnlyWhenAbsent) {
  const auto created = Apply(1, EncodeCas("c1", 1, "k", std::nullopt, "first"));
  ASSERT_TRUE(created.status.IsOk());
  EXPECT_FALSE(created.cas_mismatch);
  EXPECT_EQ(Get("k"), "first");

  const auto again = Apply(2, EncodeCas("c2", 1, "k", std::nullopt, "second"));
  ASSERT_TRUE(again.status.IsOk()) << "a failed comparison is not an error";
  EXPECT_TRUE(again.cas_mismatch);
  ASSERT_TRUE(again.value.has_value());
  EXPECT_EQ(*again.value, "first") << "the observed value should be reported back";
  EXPECT_EQ(Get("k"), "first");
}

TEST_P(StateMachineTest, CasSwapsOnAMatch) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "old")).status.IsOk());
  const auto swapped = Apply(2, EncodeCas("c1", 2, "k", "old", "new"));
  ASSERT_TRUE(swapped.status.IsOk());
  EXPECT_FALSE(swapped.cas_mismatch);
  EXPECT_EQ(Get("k"), "new");
}

TEST_P(StateMachineTest, CasRejectsAMismatch) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "actual")).status.IsOk());
  const auto rejected = Apply(2, EncodeCas("c1", 2, "k", "guessed", "new"));
  ASSERT_TRUE(rejected.status.IsOk());
  EXPECT_TRUE(rejected.cas_mismatch);
  EXPECT_EQ(Get("k"), "actual");
}

TEST_P(StateMachineTest, CasOnAnAbsentKeyWithAnExpectedValueMismatches) {
  const auto r = Apply(1, EncodeCas("c1", 1, "absent", "something", "new"));
  ASSERT_TRUE(r.status.IsOk());
  EXPECT_TRUE(r.cas_mismatch);
  EXPECT_FALSE(r.value.has_value());
  EXPECT_FALSE(Get("absent").has_value());
}

// ---------------------------------------------------------------------------
// Determinism and namespacing
// ---------------------------------------------------------------------------

// Two independent replicas fed the same command sequence must end up in the
// same state. This is the property the whole design rests on.
TEST_P(StateMachineTest, IdenticalCommandSequencesProduceIdenticalState) {
  const auto other_dir = dir_.parent_path() / (dir_.filename().string() + "_replica");
  std::filesystem::create_directories(other_dir);

  std::vector<std::string> commands = {
      EncodePut("c1", 1, "alpha", "1"), EncodePut("c2", 1, "beta", "2"),
      EncodeDelete("c1", 2, "alpha"),   EncodeCas("c3", 1, "gamma", std::nullopt, "3"),
      EncodePut("c2", 2, "beta", "22"), EncodeCas("c1", 3, "beta", "22", "222"),
  };

  for (size_t i = 0; i < commands.size(); ++i) {
    ASSERT_TRUE(Apply(i + 1, commands[i]).status.IsOk());
  }
  auto first_snapshot = sm_->TakeSnapshot(commands.size(), 1);
  ASSERT_TRUE(first_snapshot.IsOk()) << first_snapshot.GetStatus().ToString();
  auto first_bytes = (*first_snapshot)->ReadChunk(0, (*first_snapshot)->Size());
  ASSERT_TRUE(first_bytes.IsOk());
  const std::string& a = *first_bytes;

  Open(&other_dir);
  for (size_t i = 0; i < commands.size(); ++i) {
    ASSERT_TRUE(Apply(i + 1, commands[i]).status.IsOk());
  }
  auto second_snapshot = sm_->TakeSnapshot(commands.size(), 1);
  ASSERT_TRUE(second_snapshot.IsOk());
  auto second_bytes = (*second_snapshot)->ReadChunk(0, (*second_snapshot)->Size());
  ASSERT_TRUE(second_bytes.IsOk());

  EXPECT_EQ(a, *second_bytes) << "replicas diverged on identical input";

  sm_.reset();
  std::error_code ec;
  std::filesystem::remove_all(other_dir, ec);
}

// Namespace tags are prepended by the adapter, never taken from client input,
// so no request can address the metadata or session namespaces.
TEST_P(StateMachineTest, ClientKeysCannotReachInternalNamespaces) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "applied_index", "forged")).status.IsOk());
  EXPECT_EQ(sm_->LastAppliedIndex(), 1U) << "a client write clobbered the applied index";

  ASSERT_TRUE(Apply(2, EncodePut("c1", 2, "c1", "forged-session")).status.IsOk());
  auto session = sm_->LookupSession("c1");
  ASSERT_TRUE(session.IsOk());
  ASSERT_TRUE(session->has_value());
  EXPECT_EQ((*session)->last_seq(), 2U) << "a client write clobbered a session entry";
}

TEST_P(StateMachineTest, MalformedCommandsStillAdvanceTheIndex) {
  // Refusing to advance would wedge the log on a poison entry forever, and
  // every replica rejects the same bytes identically, so this stays
  // deterministic.
  const auto r = Apply(1, std::string("\xff\xff\xff not a protobuf"));
  EXPECT_FALSE(r.status.IsOk());
  EXPECT_EQ(sm_->LastAppliedIndex(), 1U);

  ASSERT_TRUE(Apply(2, EncodePut("c1", 1, "k", "v")).status.IsOk());
  EXPECT_EQ(Get("k"), "v");
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

TEST_P(StateMachineTest, SnapshotRoundTripsState) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "a", "1")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c1", 2, "b", "2")).status.IsOk());
  ASSERT_TRUE(Apply(3, EncodeDelete("c1", 3, "a")).status.IsOk());

  auto snapshot = sm_->TakeSnapshot(3, 7);
  ASSERT_TRUE(snapshot.IsOk()) << snapshot.GetStatus().ToString();
  EXPECT_EQ((*snapshot)->LastIncludedIndex(), 3U);
  EXPECT_EQ((*snapshot)->LastIncludedTerm(), 7U);

  const auto restore_dir = dir_.parent_path() / (dir_.filename().string() + "_restore");
  std::filesystem::create_directories(restore_dir);
  Open(&restore_dir);
  ASSERT_TRUE(sm_->RestoreSnapshot(**snapshot).IsOk());

  EXPECT_EQ(sm_->LastAppliedIndex(), 3U);
  EXPECT_EQ(Get("b"), "2");
  EXPECT_FALSE(Get("a").has_value()) << "a deleted key came back through the snapshot";

  sm_.reset();
  std::error_code ec;
  std::filesystem::remove_all(restore_dir, ec);
}

// Forgetting the session table in a snapshot breaks exactly-once after a
// restore: the client's retry looks new and is applied twice.
TEST_P(StateMachineTest, SnapshotCarriesTheSessionTable) {
  ASSERT_TRUE(Apply(1, EncodeCas("c1", 5, "k", std::nullopt, "created")).status.IsOk());

  auto snapshot = sm_->TakeSnapshot(1, 1);
  ASSERT_TRUE(snapshot.IsOk());

  const auto restore_dir = dir_.parent_path() / (dir_.filename().string() + "_sess");
  std::filesystem::create_directories(restore_dir);
  Open(&restore_dir);
  ASSERT_TRUE(sm_->RestoreSnapshot(**snapshot).IsOk());

  const auto retry = Apply(2, EncodeCas("c1", 5, "k", std::nullopt, "created"));
  EXPECT_TRUE(retry.deduplicated) << "dedup was lost across the snapshot";

  sm_.reset();
  std::error_code ec;
  std::filesystem::remove_all(restore_dir, ec);
}

TEST_P(StateMachineTest, RestoreReplacesRatherThanMergesState) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "kept", "1")).status.IsOk());
  auto snapshot = sm_->TakeSnapshot(1, 1);
  ASSERT_TRUE(snapshot.IsOk());

  const auto restore_dir = dir_.parent_path() / (dir_.filename().string() + "_merge");
  std::filesystem::create_directories(restore_dir);
  Open(&restore_dir);
  // Pre-existing state that the snapshot knows nothing about.
  ASSERT_TRUE(Apply(1, EncodePut("cX", 1, "stale", "should-vanish")).status.IsOk());

  ASSERT_TRUE(sm_->RestoreSnapshot(**snapshot).IsOk());
  EXPECT_EQ(Get("kept"), "1");
  EXPECT_FALSE(Get("stale").has_value()) << "restore merged instead of replacing";

  sm_.reset();
  std::error_code ec;
  std::filesystem::remove_all(restore_dir, ec);
}

TEST_P(StateMachineTest, SnapshotAtTheWrongIndexIsRejected) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v")).status.IsOk());
  auto too_far = sm_->TakeSnapshot(99, 1);
  EXPECT_FALSE(too_far.IsOk()) << "a snapshot must describe a state actually reached";
  EXPECT_EQ(too_far.GetStatus().ErrCode(), lsm::Code::kInvalidArgument);
}

TEST_P(StateMachineTest, SnapshotStreamsInChunks) {
  for (uint64_t i = 1; i <= 200; ++i) {
    ASSERT_TRUE(Apply(i, EncodePut("c1", i, "key" + std::to_string(i), std::string(500, 'v')))
                    .status.IsOk());
  }
  auto snapshot = sm_->TakeSnapshot(200, 1);
  ASSERT_TRUE(snapshot.IsOk());

  // Reassemble from small chunks, the way InstallSnapshot will in Phase 5.
  std::string assembled;
  while (assembled.size() < (*snapshot)->Size()) {
    auto chunk = (*snapshot)->ReadChunk(assembled.size(), 1024);
    ASSERT_TRUE(chunk.IsOk());
    ASSERT_FALSE(chunk->empty());
    assembled.append(*chunk);
  }
  auto whole = (*snapshot)->ReadChunk(0, (*snapshot)->Size());
  ASSERT_TRUE(whole.IsOk());
  EXPECT_EQ(assembled, *whole);
}

TEST_P(StateMachineTest, RecoversAppliedIndexAcrossRestart) {
  for (uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(Apply(i, EncodePut("c1", i, "k", std::to_string(i))).status.IsOk());
  }
  ASSERT_TRUE(sm_->Sync().IsOk());

  Open();
  EXPECT_EQ(sm_->LastAppliedIndex(), 3U);
  EXPECT_EQ(Get("k"), "3");
}

}  // namespace
// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

TEST_P(StateMachineTest, BatchAppliesEveryWriteTogether) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "gone", "x")).status.IsOk());
  const auto r =
      Apply(2, EncodeBatch("c", 2, {{}, {{"a", "1"}, {"b", "2"}, {"gone", std::nullopt}}}));
  ASSERT_TRUE(r.status.IsOk());
  EXPECT_FALSE(r.cas_mismatch);
  EXPECT_EQ(Get("a"), "1");
  EXPECT_EQ(Get("b"), "2");
  EXPECT_FALSE(Get("gone").has_value());
}

TEST_P(StateMachineTest, BatchWithAFailedCheckWritesNothing) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "taken", "mine")).status.IsOk());
  const auto r = Apply(2, EncodeBatch("c", 2,
                                      {{{"free", std::nullopt}, {"taken", std::nullopt}},
                                       {{"free", "1"}, {"other", "2"}}}));
  ASSERT_TRUE(r.status.IsOk());
  EXPECT_TRUE(r.cas_mismatch);
  ASSERT_TRUE(r.failed_check.has_value());
  EXPECT_EQ(*r.failed_check, 1U);  // the second check, on "taken"
  EXPECT_FALSE(Get("free").has_value());
  EXPECT_FALSE(Get("other").has_value());
  EXPECT_EQ(Get("taken"), "mine");
}

TEST_P(StateMachineTest, BatchChecksCompareValues) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "k", "v1")).status.IsOk());
  EXPECT_TRUE(Apply(2, EncodeBatch("c", 2, {{{"k", "v1"}}, {{"k", "v2"}}})).status.IsOk());
  EXPECT_EQ(Get("k"), "v2");
  const auto stale = Apply(3, EncodeBatch("c", 3, {{{"k", "v1"}}, {{"k", "v3"}}}));
  EXPECT_TRUE(stale.cas_mismatch);
  EXPECT_EQ(Get("k"), "v2");
}

// Checks look at the state before the batch, so a batch cannot satisfy its own
// precondition and the outcome never depends on the order of its operations.
TEST_P(StateMachineTest, BatchChecksIgnoreTheBatchsOwnWrites) {
  const auto r =
      Apply(1, EncodeBatch("c", 1, {{{"k", "from-the-batch"}}, {{"k", "from-the-batch"}}}));
  EXPECT_TRUE(r.cas_mismatch);
  EXPECT_FALSE(Get("k").has_value());
}

TEST_P(StateMachineTest, ALaterOpInABatchOverridesAnEarlierOneOnTheSameKey) {
  ASSERT_TRUE(Apply(1, EncodeBatch("c", 1, {{}, {{"k", "first"}, {"k", "second"}}})).status.IsOk());
  EXPECT_EQ(Get("k"), "second");
  ASSERT_TRUE(
      Apply(2, EncodeBatch("c", 2, {{}, {{"k", "again"}, {"k", std::nullopt}}})).status.IsOk());
  EXPECT_FALSE(Get("k").has_value());
}

TEST_P(StateMachineTest, EmptyBatchIsRejectedButACheckOnlyBatchIsValid) {
  EXPECT_FALSE(Apply(1, EncodeBatch("c", 1, {})).status.IsOk());
  const auto ok = Apply(2, EncodeBatch("c", 2, {{{"absent", std::nullopt}}, {}}));
  EXPECT_TRUE(ok.status.IsOk());
  EXPECT_FALSE(ok.cas_mismatch);
}

TEST_P(StateMachineTest, ReplayedBatchAppliesOnceAndRemembersAFailedCheck) {
  ASSERT_TRUE(Apply(1, EncodePut("c0", 1, "taken", "x")).status.IsOk());
  const std::string cmd = EncodeBatch("c", 1, {{{"taken", std::nullopt}}, {{"w", "1"}}});
  const auto first = Apply(2, cmd);
  ASSERT_TRUE(first.cas_mismatch);
  // The key is later freed; a retry of the same sequence must still report the
  // original refusal rather than re-evaluating.
  ASSERT_TRUE(Apply(3, EncodeDelete("c0", 2, "taken")).status.IsOk());
  const auto retry = Apply(4, cmd);
  EXPECT_TRUE(retry.deduplicated);
  EXPECT_TRUE(retry.cas_mismatch);
  ASSERT_TRUE(retry.failed_check.has_value());
  EXPECT_EQ(*retry.failed_check, 0U);
  EXPECT_FALSE(Get("w").has_value());
}

TEST_P(StateMachineTest, BatchSurvivesAReopen) {
  ASSERT_TRUE(Apply(1, EncodeBatch("c", 1, {{}, {{"x", "1"}, {"y", "2"}}})).status.IsOk());
  Open();
  EXPECT_EQ(Get("x"), "1");
  EXPECT_EQ(Get("y"), "2");
}

// ---------------------------------------------------------------------------
// Shard ownership
// ---------------------------------------------------------------------------

TEST_P(StateMachineTest, AnUnconfiguredMachineOwnsEverything) {
  EXPECT_EQ(sm_->CheckRange("a", "a", true), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("", "", false), RangeVerdict::kOk);
  EXPECT_TRUE(Apply(1, EncodePut("c", 1, "anything", "v")).status.IsOk());
}

TEST_P(StateMachineTest, WritesOutsideTheOwnedRangeAreRefusedAndChangeNothing) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "g", "p", false, 1))
                  .status.IsOk());

  EXPECT_TRUE(Apply(2, EncodePut("c", 1, "h", "in")).status.IsOk());
  const auto low = Apply(3, EncodePut("c", 2, "a", "out"));
  const auto high = Apply(4, EncodePut("c", 3, "p", "boundary-is-exclusive"));
  EXPECT_TRUE(low.wrong_shard);
  EXPECT_TRUE(high.wrong_shard);
  EXPECT_EQ(Get("h"), "in");
  EXPECT_FALSE(Get("a").has_value());
  EXPECT_FALSE(Get("p").has_value());

  EXPECT_TRUE(Apply(5, EncodeCas("c", 4, "zzz", std::nullopt, "x")).wrong_shard);
  EXPECT_TRUE(Apply(6, EncodeDelete("c", 5, "a")).wrong_shard);
  // One stray key anywhere in a batch refuses the whole batch.
  EXPECT_TRUE(Apply(7, EncodeBatch("c", 6, {{}, {{"h", "x"}, {"a", "y"}}})).wrong_shard);
  EXPECT_EQ(Get("h"), "in");
}

TEST_P(StateMachineTest, CheckRangeJudgesReadsAndScans) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "g", "p", false, 1))
                  .status.IsOk());
  EXPECT_EQ(sm_->CheckRange("h", "h", false), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("a", "a", false), RangeVerdict::kWrongShard);
  EXPECT_EQ(sm_->CheckRange("g", "p", false), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("h", "z", false), RangeVerdict::kWrongShard);  // spills past the end
  EXPECT_EQ(sm_->CheckRange("h", "", false), RangeVerdict::kWrongShard);   // unbounded spills too
  EXPECT_EQ(sm_->CheckRange("a", "h", false), RangeVerdict::kWrongShard);
}

// Start "" and end "" mean "the whole key space" for a scan, but for a point
// check they mean "the key empty string". A shard that had given away the top
// of its range answered an unbounded scan with only the part it still held,
// because the scan was judged as a single-key lookup. The span forms always
// treat the arguments as a range.
TEST_P(StateMachineTest, AnUnboundedScanIsNotMistakenForAPointLookup) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "m", false, 1))
                  .status.IsOk());
  EXPECT_EQ(sm_->CheckSpan("", ""), RangeVerdict::kWrongShard)
      << "the range ends at m, a scan to the end spills past it";
  EXPECT_EQ(sm_->CheckSpan("", "m"), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckSpan("a", "z"), RangeVerdict::kWrongShard);
  EXPECT_EQ(sm_->CheckRange("", "", false), RangeVerdict::kOk)
      << "as a point, the key \"\" is owned";
}

TEST_P(StateMachineTest, AnUnboundedScanSeesALockThatAPointCheckOfTheEmptyKeyDoesNot) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "a", "old")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePrewrite("t", 1, "T1", "a", {{"a", proto::LockRecord::PUT, "new"}}))
                  .status.IsOk());
  EXPECT_FALSE(sm_->FindBlockingLock("", "").has_value()) << "this is a lookup of the key \"\"";
  EXPECT_TRUE(sm_->FindBlockingLockInSpan("", "").has_value())
      << "a scan of everything must meet the lock on a";
}

TEST_P(StateMachineTest, AnUnboundedRangeOwnsEverythingFromItsStart) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "m", "", true, 1))
                  .status.IsOk());
  EXPECT_EQ(sm_->CheckRange("zzzz", "zzzz", false), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("m", "", false), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("l", "l", false), RangeVerdict::kWrongShard);
}

TEST_P(StateMachineTest, AFenceRefusesWritesButStillAllowsReads) {
  ASSERT_TRUE(
      Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "", true, 1)).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c", 1, "k", "before")).status.IsOk());
  ASSERT_TRUE(
      Apply(3, EncodeRange("admin", 2, proto::RangeCmd::FENCE, "j", "", true, 2)).status.IsOk());

  EXPECT_TRUE(Apply(4, EncodePut("c", 2, "k", "during")).moving);
  EXPECT_TRUE(Apply(5, EncodePut("c", 3, "a", "below-the-fence")).status.IsOk());  // not fenced
  EXPECT_EQ(Get("k"), "before");
  EXPECT_EQ(sm_->CheckRange("k", "k", false), RangeVerdict::kOk);  // reads continue
  EXPECT_EQ(sm_->CheckRange("k", "k", true), RangeVerdict::kMoving);
}

TEST_P(StateMachineTest, ShrinkingTheOwnedRangeEndsTheFenceAndRefusesTheMovedKeys) {
  ASSERT_TRUE(
      Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "", true, 1)).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c", 1, "k", "v")).status.IsOk());
  ASSERT_TRUE(
      Apply(3, EncodeRange("admin", 2, proto::RangeCmd::FENCE, "j", "", true, 2)).status.IsOk());
  ASSERT_TRUE(Apply(4, EncodeRange("admin", 3, proto::RangeCmd::SET_OWNED, "", "j", false, 2))
                  .status.IsOk());

  EXPECT_TRUE(Apply(5, EncodePut("c", 2, "k", "moved")).wrong_shard);
  EXPECT_EQ(sm_->CheckRange("k", "k", false), RangeVerdict::kWrongShard);
  EXPECT_EQ(sm_->CheckRange("a", "a", true), RangeVerdict::kOk);  // fence is gone
}

TEST_P(StateMachineTest, ImportBypassesOwnershipSoATargetCanBeFilledBeforeItServes) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "", false, 1, true))
                  .status.IsOk());
  EXPECT_TRUE(Apply(2, EncodePut("c", 1, "k", "x")).wrong_shard);  // owns nothing yet
  ASSERT_TRUE(
      Apply(3, EncodeImport("admin", 2, {{"k", "copied"}, {"l", "copied2"}})).status.IsOk());
  ASSERT_TRUE(Apply(4, EncodeRange("admin", 3, proto::RangeCmd::SET_OWNED, "j", "", true, 2))
                  .status.IsOk());
  EXPECT_EQ(Get("k"), "copied");
  EXPECT_TRUE(Apply(5, EncodePut("c", 2, "k", "live")).status.IsOk());
  EXPECT_EQ(Get("k"), "live");
}

TEST_P(StateMachineTest, AStaleEpochCannotUndoANewerHandover) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "m", false, 5))
                  .status.IsOk());
  const auto stale = Apply(2, EncodeRange("admin", 2, proto::RangeCmd::SET_OWNED, "", "", true, 3));
  EXPECT_FALSE(stale.status.IsOk());
  EXPECT_EQ(sm_->CheckRange("z", "z", false), RangeVerdict::kWrongShard);
  // The same epoch is fine, which is what makes a coordinator's retry safe.
  EXPECT_TRUE(Apply(3, EncodeRange("admin", 3, proto::RangeCmd::SET_OWNED, "", "m", false, 5))
                  .status.IsOk());
}

TEST_P(StateMachineTest, FencingAnUnconfiguredMachineMeansItOwnedEverything) {
  ASSERT_TRUE(
      Apply(1, EncodeRange("admin", 1, proto::RangeCmd::FENCE, "m", "", true, 1)).status.IsOk());
  EXPECT_EQ(sm_->CheckRange("a", "a", true), RangeVerdict::kOk);
  EXPECT_EQ(sm_->CheckRange("z", "z", true), RangeVerdict::kMoving);
  EXPECT_EQ(sm_->CheckRange("z", "z", false), RangeVerdict::kOk);
}

TEST_P(StateMachineTest, CannotFenceWhatIsNotOwned) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "", "", false, 1, true))
                  .status.IsOk());
  EXPECT_FALSE(
      Apply(2, EncodeRange("admin", 2, proto::RangeCmd::FENCE, "a", "b", false, 2)).status.IsOk());
}

TEST_P(StateMachineTest, OwnershipSurvivesAReopenAndASnapshotRestore) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "g", "p", false, 4))
                  .status.IsOk());
  ASSERT_TRUE(
      Apply(2, EncodeRange("admin", 2, proto::RangeCmd::FENCE, "m", "p", false, 4)).status.IsOk());
  Open();
  EXPECT_EQ(sm_->CheckRange("a", "a", false), RangeVerdict::kWrongShard);
  EXPECT_EQ(sm_->CheckRange("n", "n", true), RangeVerdict::kMoving);

  auto snapshot = sm_->TakeSnapshot(sm_->LastAppliedIndex(), 1);
  ASSERT_TRUE(snapshot.IsOk());
  const std::filesystem::path other = dir_ / "restored";
  std::filesystem::create_directories(other);
  Open(&other);
  ASSERT_TRUE(sm_->RestoreSnapshot(**snapshot).IsOk());
  EXPECT_EQ(sm_->CheckRange("a", "a", false), RangeVerdict::kWrongShard);
  EXPECT_EQ(sm_->CheckRange("n", "n", true), RangeVerdict::kMoving);
  EXPECT_EQ(sm_->CheckRange("h", "h", true), RangeVerdict::kOk);
}

TEST_P(StateMachineTest, ARefusedWriteIsCachedSoARetryGivesTheSameAnswer) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "g", "p", false, 1))
                  .status.IsOk());
  const std::string cmd = EncodePut("c", 1, "a", "x");
  EXPECT_TRUE(Apply(2, cmd).wrong_shard);
  const auto retry = Apply(3, cmd);
  EXPECT_TRUE(retry.deduplicated);
  EXPECT_TRUE(retry.wrong_shard);
}

TEST_P(StateMachineTest, PurgeDeletesGivenAwayKeysAndNeverLiveOnes) {
  for (char c = 'a'; c <= 'z'; ++c) {
    ASSERT_TRUE(Apply(static_cast<uint64_t>(c),
                      EncodePut("c", static_cast<uint64_t>(c), std::string(1, c), "v"))
                    .status.IsOk());
  }
  uint64_t seq = 1;
  auto purge = [&](uint64_t index, const std::string& lo, const std::string& hi, bool unbounded,
                   uint32_t limit) {
    proto::Command c;
    c.set_client_id("admin");
    c.set_seq(seq++);
    auto* r = c.mutable_range();
    r->set_kind(proto::RangeCmd::PURGE);
    r->set_lo(lo);
    r->set_hi(hi);
    r->set_unbounded_hi(unbounded);
    r->set_limit(limit);
    r->set_epoch(1);
    return Apply(index, c.SerializeAsString());
  };
  // Own [a, m): purging inside it is refused and deletes nothing.
  ASSERT_TRUE(Apply(1000, EncodeRange("o", 1, proto::RangeCmd::SET_OWNED, "a", "m", false, 1))
                  .status.IsOk());
  EXPECT_FALSE(purge(1001, "k", "q", false, 0).status.IsOk());
  EXPECT_EQ(Get("k"), "v");

  // [m, unbounded) is not ours: purge it in pages of 5.
  size_t total = 0;
  for (uint64_t i = 0; i < 10; ++i) {
    const auto r = purge(1002 + i, "m", "", true, 5);
    ASSERT_TRUE(r.status.IsOk());
    const size_t n = std::stoul(*r.value);
    total += n;
    if (n == 0) break;
  }
  EXPECT_EQ(total, 14U);  // m..z
  EXPECT_FALSE(Get("m").has_value());
  EXPECT_FALSE(Get("z").has_value());
  EXPECT_EQ(Get("a"), "v");
  EXPECT_EQ(Get("l"), "v");
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

TEST_P(StateMachineTest, PrewriteLocksWithoutChangingValuesAndCommitInstallsThem) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "a", "old")).status.IsOk());
  const auto pre = Apply(2, EncodePrewrite("t", 1, "T1", "a",
                                           {{"a", proto::LockRecord::PUT, "new-a"},
                                            {"b", proto::LockRecord::PUT, "new-b"}}));
  ASSERT_TRUE(pre.status.IsOk());
  EXPECT_EQ(pre.txn, TxnOutcome::kNone);
  EXPECT_EQ(Get("a"), "old") << "a prewrite must not change visible values";
  EXPECT_FALSE(Get("b").has_value());
  EXPECT_TRUE(sm_->FindBlockingLock("a", "a").has_value());

  const auto commit = Apply(3, EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"a", "b"}, true));
  ASSERT_TRUE(commit.status.IsOk());
  EXPECT_EQ(commit.txn, TxnOutcome::kNone);
  EXPECT_EQ(Get("a"), "new-a");
  EXPECT_EQ(Get("b"), "new-b");
  EXPECT_FALSE(sm_->FindBlockingLock("a", "a").has_value());
  auto decision = sm_->TxnDecision("T1");
  ASSERT_TRUE(decision.IsOk());
  EXPECT_EQ(*decision, std::optional<std::string>("C"));
}

TEST_P(StateMachineTest, ALockOnlyKeyHoldsOffWritersButIsInvisibleToReaders) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "r", "v")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePrewrite("t", 1, "T1", "w",
                                      {{"r", proto::LockRecord::LOCK_ONLY, ""},
                                       {"w", proto::LockRecord::PUT, "x"}}))
                  .status.IsOk());
  EXPECT_FALSE(sm_->FindBlockingLock("r", "r").has_value());
  EXPECT_TRUE(sm_->FindBlockingLock("w", "w").has_value());
  EXPECT_EQ(Apply(3, EncodePut("c", 2, "r", "sneaky")).txn, TxnOutcome::kLocked);
  EXPECT_EQ(Get("r"), "v");
  // Committing releases the read-only lock without writing anything to it.
  ASSERT_TRUE(
      Apply(4, EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"r", "w"}, true)).status.IsOk());
  EXPECT_EQ(Get("r"), "v");
  EXPECT_TRUE(Apply(5, EncodePut("c", 3, "r", "now-ok")).status.IsOk());
}

TEST_P(StateMachineTest, ConflictingPrewriteIsRefusedAndLocksNothing) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "1"}}))
                  .status.IsOk());
  const auto r = Apply(2, EncodePrewrite("u", 1, "T2", "free",
                                         {{"free", proto::LockRecord::PUT, "x"},
                                          {"k", proto::LockRecord::PUT, "2"}}));
  EXPECT_EQ(r.txn, TxnOutcome::kLocked);
  ASSERT_TRUE(r.lock.has_value());
  EXPECT_EQ(r.lock->txid(), "T1");
  EXPECT_EQ(r.lock->primary_key(), "k");
  EXPECT_FALSE(sm_->FindBlockingLock("free", "free").has_value())
      << "the refused prewrite locked a key";
  EXPECT_TRUE(Apply(3, EncodePut("c", 1, "free", "ok")).status.IsOk());
}

TEST_P(StateMachineTest, PrewriteValidatesWhatTheTransactionRead) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "k", "v1")).status.IsOk());
  TxnKeySpec good{"k", proto::LockRecord::PUT, "v2", true, false, "v1"};
  TxnKeySpec stale{"k", proto::LockRecord::PUT, "v2", true, false, "v0"};
  TxnKeySpec absent{"nope", proto::LockRecord::PUT, "x", true, true, ""};
  TxnKeySpec wrong_absent{"k", proto::LockRecord::PUT, "x", true, true, ""};

  const auto bad = Apply(2, EncodePrewrite("t", 1, "T1", "k", {absent, stale}));
  EXPECT_EQ(bad.txn, TxnOutcome::kValidationFailed);
  ASSERT_TRUE(bad.failed_check.has_value());
  EXPECT_EQ(*bad.failed_check, 1U);
  EXPECT_FALSE(sm_->FindBlockingLock("nope", "nope").has_value()) << "validation is all or nothing";
  EXPECT_EQ(Apply(3, EncodePrewrite("t", 2, "T2", "k", {wrong_absent})).txn,
            TxnOutcome::kValidationFailed);
  EXPECT_EQ(Apply(4, EncodePrewrite("t", 3, "T3", "k", {good, absent})).txn, TxnOutcome::kNone);
}

TEST_P(StateMachineTest, PlainWritesToALockedKeyAreRefusedInEveryForm) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "tx"}}))
                  .status.IsOk());
  EXPECT_EQ(Apply(2, EncodePut("c", 1, "k", "x")).txn, TxnOutcome::kLocked);
  EXPECT_EQ(Apply(3, EncodeDelete("c", 2, "k")).txn, TxnOutcome::kLocked);
  EXPECT_EQ(Apply(4, EncodeCas("c", 3, "k", std::nullopt, "x")).txn, TxnOutcome::kLocked);
  EXPECT_EQ(Apply(5, EncodeBatch("c", 4, {{}, {{"other", "1"}, {"k", "2"}}})).txn,
            TxnOutcome::kLocked);
  EXPECT_EQ(Apply(6, EncodeBatch("c", 5, {{{"k", std::nullopt}}, {{"other", "1"}}})).txn,
            TxnOutcome::kLocked);
  EXPECT_FALSE(Get("other").has_value()) << "a refused batch wrote part of itself";
  EXPECT_TRUE(Apply(7, EncodePut("c", 6, "unrelated", "fine")).status.IsOk());
}

TEST_P(StateMachineTest, RollbackReleasesLocksAndBlocksALatePrewrite) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "a",
                                      {{"a", proto::LockRecord::PUT, "x"},
                                       {"b", proto::LockRecord::PUT, "y"}}))
                  .status.IsOk());
  const auto rb = Apply(2, EncodeTxnEnd("o", 1, proto::TxnCmd::ROLLBACK, "T1", {"a", "b"}));
  ASSERT_TRUE(rb.status.IsOk());
  EXPECT_FALSE(sm_->FindBlockingLock("a", "a").has_value());
  EXPECT_TRUE(Apply(3, EncodePut("c", 1, "a", "free")).status.IsOk());
  auto decision = sm_->TxnDecision("T1");
  ASSERT_TRUE(decision.IsOk());
  EXPECT_EQ(*decision, std::optional<std::string>("A"));

  // The coordinator's delayed prewrite shows up after the rollback.
  EXPECT_EQ(
      Apply(4, EncodePrewrite("t", 2, "T1", "a", {{"a", proto::LockRecord::PUT, "late"}})).txn,
      TxnOutcome::kAborted);
  EXPECT_FALSE(sm_->FindBlockingLock("a", "a").has_value()) << "a late prewrite resurrected a lock";
  // And committing the primary of a rolled-back transaction fails.
  EXPECT_EQ(Apply(5, EncodeTxnEnd("t", 3, proto::TxnCmd::COMMIT, "T1", {"a"}, true)).txn,
            TxnOutcome::kAborted);
  EXPECT_EQ(Get("a"), "free");
}

TEST_P(StateMachineTest, ACommittedTransactionCannotBeRolledBack) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "v"}}))
                  .status.IsOk());
  ASSERT_TRUE(
      Apply(2, EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"k"}, true)).status.IsOk());
  EXPECT_EQ(Apply(3, EncodeTxnEnd("o", 1, proto::TxnCmd::ROLLBACK, "T1", {"k"})).txn,
            TxnOutcome::kAlreadyCommitted);
  EXPECT_EQ(Get("k"), "v");
  auto decision = sm_->TxnDecision("T1");
  EXPECT_EQ(*decision, std::optional<std::string>("C"));
}

TEST_P(StateMachineTest, CommitIsIdempotentAndNeverReappliesAnOldValue) {
  ASSERT_TRUE(
      Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "from-txn"}}))
          .status.IsOk());
  ASSERT_TRUE(
      Apply(2, EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"k"}, true)).status.IsOk());
  ASSERT_TRUE(Apply(3, EncodePut("c", 1, "k", "later-write")).status.IsOk());
  // A resolver and the coordinator both roll the key forward; the second must
  // not clobber what happened since.
  const auto again = Apply(4, EncodeTxnEnd("r", 1, proto::TxnCmd::COMMIT, "T1", {"k"}));
  EXPECT_TRUE(again.status.IsOk());
  EXPECT_EQ(again.txn, TxnOutcome::kNone);
  EXPECT_EQ(Get("k"), "later-write");
}

TEST_P(StateMachineTest, CommittingAKeyWhoseLockWasRolledBackReportsAbort) {
  EXPECT_EQ(Apply(1, EncodeTxnEnd("t", 1, proto::TxnCmd::COMMIT, "T9", {"k"}, false)).txn,
            TxnOutcome::kAborted);
  EXPECT_FALSE(Get("k").has_value());
}

TEST_P(StateMachineTest, DeleteIntentRemovesTheKeyOnCommit) {
  ASSERT_TRUE(Apply(1, EncodePut("c", 1, "k", "v")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::DELETE, ""}}))
                  .status.IsOk());
  EXPECT_EQ(Get("k"), "v");
  ASSERT_TRUE(
      Apply(3, EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"k"}, true)).status.IsOk());
  EXPECT_FALSE(Get("k").has_value());
}

TEST_P(StateMachineTest, LocksAndDecisionsSurviveAReopenAndASnapshotRestore) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "v"}}))
                  .status.IsOk());
  Open();
  EXPECT_EQ(Apply(2, EncodePut("c", 1, "k", "x")).txn, TxnOutcome::kLocked)
      << "the lock count was lost on reopen";

  auto snapshot = sm_->TakeSnapshot(sm_->LastAppliedIndex(), 1);
  ASSERT_TRUE(snapshot.IsOk());
  const std::filesystem::path other = dir_ / "restored";
  std::filesystem::create_directories(other);
  Open(&other);
  ASSERT_TRUE(sm_->RestoreSnapshot(**snapshot).IsOk());
  EXPECT_EQ(Apply(sm_->LastAppliedIndex() + 1, EncodePut("c", 2, "k", "x")).txn,
            TxnOutcome::kLocked);
  ASSERT_TRUE(Apply(sm_->LastAppliedIndex() + 1,
                    EncodeTxnEnd("t", 2, proto::TxnCmd::COMMIT, "T1", {"k"}, true))
                  .status.IsOk());
  EXPECT_EQ(Get("k"), "v");
}

TEST_P(StateMachineTest, ARangeWithATransactionInFlightCannotBeFenced) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "m", {{"m", proto::LockRecord::PUT, "v"}}))
                  .status.IsOk());
  const auto fence = Apply(2, EncodeRange("admin", 1, proto::RangeCmd::FENCE, "k", "", true, 1));
  EXPECT_FALSE(fence.status.IsOk());
  // Below the lock is fine.
  EXPECT_TRUE(
      Apply(3, EncodeRange("admin", 2, proto::RangeCmd::FENCE, "a", "k", false, 1)).status.IsOk());
}

TEST_P(StateMachineTest, TransactionsRespectShardOwnership) {
  ASSERT_TRUE(Apply(1, EncodeRange("admin", 1, proto::RangeCmd::SET_OWNED, "g", "p", false, 1))
                  .status.IsOk());
  EXPECT_TRUE(Apply(2, EncodePrewrite("t", 1, "T1", "a", {{"a", proto::LockRecord::PUT, "x"}}))
                  .wrong_shard);
  EXPECT_FALSE(sm_->FindBlockingLock("a", "a").has_value());
}

TEST_P(StateMachineTest, ARefusedPrewriteIsCachedForRetries) {
  ASSERT_TRUE(Apply(1, EncodePrewrite("t", 1, "T1", "k", {{"k", proto::LockRecord::PUT, "1"}}))
                  .status.IsOk());
  const std::string cmd = EncodePrewrite("u", 1, "T2", "k", {{"k", proto::LockRecord::PUT, "2"}});
  const auto first = Apply(2, cmd);
  ASSERT_EQ(first.txn, TxnOutcome::kLocked);
  const auto retry = Apply(3, cmd);
  EXPECT_TRUE(retry.deduplicated);
  EXPECT_EQ(retry.txn, TxnOutcome::kLocked);
  ASSERT_TRUE(retry.lock.has_value());
  EXPECT_EQ(retry.lock->txid(), "T1");
}

INSTANTIATE_TEST_SUITE_P(Engines, StateMachineTest, ::testing::ValuesIn(engine::AvailableKinds()),
                         [](const ::testing::TestParamInfo<engine::Kind>& info) {
                           return std::string(engine::KindName(info.param));
                         });

}  // namespace raftkv::statemachine
