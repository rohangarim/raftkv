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

class StateMachineTest : public ::testing::Test {
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
    lsm::Options o;
    o.dir = dir != nullptr ? *dir : dir_;
    o.memtable_bytes = size_t{64} * 1024 * 1024;
    auto opened = KvStateMachine::Open(std::move(o));
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

TEST_F(StateMachineTest, AppliesAPut) {
  const auto r = Apply(1, EncodePut("c1", 1, "k", "v"));
  ASSERT_TRUE(r.status.IsOk()) << r.status.ToString();
  EXPECT_FALSE(r.deduplicated);
  EXPECT_EQ(Get("k"), "v");
  EXPECT_EQ(sm_->LastAppliedIndex(), 1U);
}

TEST_F(StateMachineTest, AppliesADelete) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodeDelete("c1", 2, "k")).status.IsOk());
  EXPECT_FALSE(Get("k").has_value());
}

TEST_F(StateMachineTest, AdvancesTheAppliedIndex) {
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
TEST_F(StateMachineTest, ReplayingASequenceAppliesOnlyOnce) {
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

TEST_F(StateMachineTest, DedupReturnsTheOriginalCachedValue) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "original")).status.IsOk());
  // A different client changes the key in between.
  ASSERT_TRUE(Apply(2, EncodePut("c2", 1, "k", "changed")).status.IsOk());

  const auto retry = Apply(3, EncodePut("c1", 1, "k", "original"));
  EXPECT_TRUE(retry.deduplicated);
  EXPECT_EQ(Get("k"), "changed") << "the retry must not have re-applied its write";
}

TEST_F(StateMachineTest, DifferentClientsDoNotShareASequenceSpace) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "a", "1")).status.IsOk());
  const auto other = Apply(2, EncodePut("c2", 1, "b", "2"));
  EXPECT_FALSE(other.deduplicated) << "c2 seq 1 is unrelated to c1 seq 1";
  EXPECT_EQ(Get("b"), "2");
}

TEST_F(StateMachineTest, AnOlderSequenceIsRejectedRatherThanReapplied) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v1")).status.IsOk());
  ASSERT_TRUE(Apply(2, EncodePut("c1", 2, "k", "v2")).status.IsOk());

  // Only the most recent result is cached, so seq 1 can no longer be answered.
  // Re-applying it would silently roll the key back to v1.
  const auto stale = Apply(3, EncodePut("c1", 1, "k", "v1"));
  EXPECT_FALSE(stale.status.IsOk()) << "a stale sequence must be an explicit error";
  EXPECT_EQ(Get("k"), "v2");
}

TEST_F(StateMachineTest, DedupSurvivesRestart) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 7, "k", "v")).status.IsOk());
  ASSERT_TRUE(sm_->Sync().IsOk());

  Open();
  const auto retry = Apply(2, EncodePut("c1", 7, "k", "v"));
  EXPECT_TRUE(retry.deduplicated) << "the session table did not survive recovery";
}

TEST_F(StateMachineTest, CommandsWithoutAClientIdAreNotDeduplicated) {
  // Internal commands (a leader's no-op entry, for instance) carry no client.
  const auto first = Apply(1, EncodePut("", 0, "k", "v1"));
  ASSERT_TRUE(first.status.IsOk());
  const auto second = Apply(2, EncodePut("", 0, "k", "v2"));
  EXPECT_FALSE(second.deduplicated);
  EXPECT_EQ(Get("k"), "v2");
}

// Recovery replays the tail of the Raft log. An index at or below the applied
// index must be ignored, or every restart re-applies its last commands.
TEST_F(StateMachineTest, ReplayingAnOldIndexIsIgnored) {
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

TEST_F(StateMachineTest, CasCreatesOnlyWhenAbsent) {
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

TEST_F(StateMachineTest, CasSwapsOnAMatch) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "old")).status.IsOk());
  const auto swapped = Apply(2, EncodeCas("c1", 2, "k", "old", "new"));
  ASSERT_TRUE(swapped.status.IsOk());
  EXPECT_FALSE(swapped.cas_mismatch);
  EXPECT_EQ(Get("k"), "new");
}

TEST_F(StateMachineTest, CasRejectsAMismatch) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "actual")).status.IsOk());
  const auto rejected = Apply(2, EncodeCas("c1", 2, "k", "guessed", "new"));
  ASSERT_TRUE(rejected.status.IsOk());
  EXPECT_TRUE(rejected.cas_mismatch);
  EXPECT_EQ(Get("k"), "actual");
}

TEST_F(StateMachineTest, CasOnAnAbsentKeyWithAnExpectedValueMismatches) {
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
TEST_F(StateMachineTest, IdenticalCommandSequencesProduceIdenticalState) {
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
  const std::string a = *first_bytes;

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
TEST_F(StateMachineTest, ClientKeysCannotReachInternalNamespaces) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "applied_index", "forged")).status.IsOk());
  EXPECT_EQ(sm_->LastAppliedIndex(), 1U) << "a client write clobbered the applied index";

  ASSERT_TRUE(Apply(2, EncodePut("c1", 2, "c1", "forged-session")).status.IsOk());
  auto session = sm_->LookupSession("c1");
  ASSERT_TRUE(session.IsOk());
  ASSERT_TRUE(session->has_value());
  EXPECT_EQ((*session)->last_seq(), 2U) << "a client write clobbered a session entry";
}

TEST_F(StateMachineTest, MalformedCommandsStillAdvanceTheIndex) {
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

TEST_F(StateMachineTest, SnapshotRoundTripsState) {
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
TEST_F(StateMachineTest, SnapshotCarriesTheSessionTable) {
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

TEST_F(StateMachineTest, RestoreReplacesRatherThanMergesState) {
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

TEST_F(StateMachineTest, SnapshotAtTheWrongIndexIsRejected) {
  ASSERT_TRUE(Apply(1, EncodePut("c1", 1, "k", "v")).status.IsOk());
  auto too_far = sm_->TakeSnapshot(99, 1);
  EXPECT_FALSE(too_far.IsOk()) << "a snapshot must describe a state actually reached";
  EXPECT_EQ(too_far.GetStatus().ErrCode(), lsm::Code::kInvalidArgument);
}

TEST_F(StateMachineTest, SnapshotStreamsInChunks) {
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

TEST_F(StateMachineTest, RecoversAppliedIndexAcrossRestart) {
  for (uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(Apply(i, EncodePut("c1", i, "k", std::to_string(i))).status.IsOk());
  }
  ASSERT_TRUE(sm_->Sync().IsOk());

  Open();
  EXPECT_EQ(sm_->LastAppliedIndex(), 3U);
  EXPECT_EQ(Get("k"), "3");
}

}  // namespace
}  // namespace raftkv::statemachine
