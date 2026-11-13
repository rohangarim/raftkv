#pragma once

// The replicated key-value state machine, backed by the LSM engine.
//
// Key namespacing. Client keys are arbitrary bytes, so metadata cannot be
// distinguished by a magic prefix a client might also produce. Every key
// stored in the engine carries a one-byte namespace tag:
//
//   'u' + user_key     client data
//   'm' + name         engine metadata (the applied index)
//   's' + client_id    client session entries
//
// Because the tag is prepended by this layer and never taken from client
// input, no request can address the metadata or session namespaces.
//
// Atomicity. A single Apply() writes the user effect, the updated session
// entry, and the new applied index in ONE WriteBatch, therefore in one WAL
// record behind one CRC. A crash can leave all three or none. Anything weaker
// breaks exactly-once: an effect without a session update is applied twice on
// retry, and a session update without its effect silently drops a write.

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "kv.pb.h"
#include "lsm/db.h"

#include "statemachine/state_machine.h"

namespace raftkv::statemachine {

// Namespace tags. Values are arbitrary but must stay stable: changing one
// makes every existing database unreadable.
inline constexpr char kUserPrefix = 'u';
inline constexpr char kMetaPrefix = 'm';
inline constexpr char kSessionPrefix = 's';
inline constexpr char kLockPrefix = 'l';    // l<key> -> LockRecord
inline constexpr char kTxnPrefix = 'x';     // x<txid> -> 'C' or 'A'

inline constexpr std::string_view kAppliedIndexKey = "applied_index";

class KvStateMachine final : public StateMachine {
 public:
  static Result<std::unique_ptr<KvStateMachine>> Open(lsm::Options options);

  ApplyResult Apply(uint64_t index, std::span<const std::byte> command) override;
  Result<std::unique_ptr<Snapshot>> TakeSnapshot(uint64_t index, uint64_t term) override;
  Status RestoreSnapshot(Snapshot& snapshot) override;
  uint64_t LastAppliedIndex() const override;
  Status Scan(std::string_view start, std::string_view end, size_t limit,
              const std::function<Status(std::string_view, std::string_view)>& fn) const override;
  RangeVerdict CheckRange(std::string_view start, std::string_view end,
                          bool write) const override;
  std::optional<proto::LockRecord> FindBlockingLock(std::string_view start,
                                                    std::string_view end) const override;
  Result<std::optional<std::string>> TxnDecision(std::string_view txid) const override;
  Status Sync() override;

  // Direct read path, used by linearizable reads in Phase 4. Does not go
  // through the log and does not touch the session table.
  Result<std::optional<std::string>> Get(std::string_view key) const override;

  // Test introspection.
  Result<std::optional<proto::SessionEntry>> LookupSession(const std::string& client_id) const;

 private:
  explicit KvStateMachine(std::unique_ptr<lsm::DB> db);

  static std::string UserKey(std::string_view key);
  static std::string SessionKey(std::string_view client_id);
  static std::string MetaKey(std::string_view name);

  Status LoadAppliedIndex();
  Status LoadRange();
  Status LoadLockCount();

  static std::string LockKey(std::string_view key);
  static std::string TxnKey(std::string_view txid);
  Result<std::optional<proto::LockRecord>> GetLock(std::string_view key) const;
  // The first lock that blocks a plain write to any key `command` touches.
  ApplyResult CheckWriteLocks(const proto::Command& command, bool* blocked) const;
  ApplyResult PrepareTxn(const proto::TxnCmd& cmd, lsm::WriteBatch* batch);

  // Judges one key against the owned and fenced ranges. Caller holds mutex_.
  RangeVerdict JudgeKey(std::string_view key, bool write) const;
  // The strictest verdict over every key a command touches.
  RangeVerdict JudgeCommand(const proto::Command& command) const;
  // Applies a RangeCmd to `next`, filling `batch` with the persisted result.
  ApplyResult PrepareRange(const proto::RangeCmd& cmd, lsm::WriteBatch* batch);

  // Records that `index` has been consumed without changing any user data, and
  // returns `result` unchanged unless that write itself fails. Caller holds
  // mutex_.
  ApplyResult AdvanceOnly(uint64_t index, ApplyResult result);

  // Executes the operation and fills `batch`; does not commit.
  ApplyResult Prepare(const proto::Command& command, lsm::WriteBatch* batch);

  std::unique_ptr<lsm::DB> db_;

  // Apply() is single-threaded by contract -- one Ready loop per Raft group
  // drives it -- but Get() and LastAppliedIndex() are served from gRPC handler
  // threads, so the applied index needs protection.
  mutable std::mutex mutex_;
  uint64_t last_applied_index_ = 0;

  // Which keys this group serves. Unconfigured means all of them, so a plain
  // single-group cluster needs no setup. Guarded by mutex_.
  proto::RangeState range_;
  // Set by Prepare when a command changes `range_`; installed by Apply only
  // once the write that persists it has succeeded.
  std::optional<proto::RangeState> pending_range_;

  // How many locks exist, so the common case of none costs nothing per write.
  // Guarded by mutex_; changes are staged in pending_lock_delta_ like the range.
  int64_t lock_count_ = 0;
  int64_t pending_lock_delta_ = 0;
};

}  // namespace raftkv::statemachine
