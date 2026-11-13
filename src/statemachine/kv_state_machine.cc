#include "statemachine/kv_state_machine.h"

#include <algorithm>
#include <utility>

#include "lsm/coding.h"

#include "common/log.h"

namespace raftkv::statemachine {
namespace {

ApplyResult ResultFromCached(const proto::CommandResult& cached) {
  ApplyResult out;
  out.deduplicated = true;
  switch (cached.code()) {
    case proto::CommandResult::OK:
      out.status = Status::Ok();
      break;
    case proto::CommandResult::CAS_MISMATCH:
      out.status = Status::Ok();
      out.cas_mismatch = true;
      if (cached.has_failed_check()) {
        out.failed_check = cached.failed_check();
      }
      break;
    case proto::CommandResult::NOT_FOUND:
      out.status = Status::Ok();
      break;
    case proto::CommandResult::WRONG_SHARD:
      out.status = Status::Ok();
      out.wrong_shard = true;
      break;
    case proto::CommandResult::MOVING:
      out.status = Status::Ok();
      out.moving = true;
      break;
    case proto::CommandResult::LOCKED:
      out.status = Status::Ok();
      out.txn = TxnOutcome::kLocked;
      if (cached.has_lock()) {
        out.lock = cached.lock();
      }
      break;
    case proto::CommandResult::VALIDATION_FAILED:
      out.status = Status::Ok();
      out.txn = TxnOutcome::kValidationFailed;
      if (cached.has_failed_check()) {
        out.failed_check = cached.failed_check();
      }
      break;
    case proto::CommandResult::TXN_ABORTED:
      out.status = Status::Ok();
      out.txn = TxnOutcome::kAborted;
      break;
    case proto::CommandResult::ALREADY_COMMITTED:
      out.status = Status::Ok();
      out.txn = TxnOutcome::kAlreadyCommitted;
      break;
    default:
      out.status = Status::InvalidArgument("cached result: invalid command");
      break;
  }
  if (cached.has_value()) {
    out.value = cached.value();
  }
  return out;
}

// FNV-1a. Not cryptographic and not meant to be: this only has to tell a byte-
// identical retry from a different command under a reused sequence number, and
// it must be deterministic so every replica reaches the same verdict.
uint64_t Digest(std::span<const std::byte> bytes) {
  uint64_t h = 1469598103934665603ULL;
  for (std::byte b : bytes) {
    h ^= static_cast<uint8_t>(b);
    h *= 1099511628211ULL;
  }
  return h == 0 ? 1 : h;  // 0 is reserved for "not recorded"
}

proto::CommandResult CacheableResult(const ApplyResult& result) {
  proto::CommandResult cached;
  if (!result.status.IsOk()) {
    cached.set_code(proto::CommandResult::INVALID);
    return cached;
  }
  if (result.wrong_shard) {
    cached.set_code(proto::CommandResult::WRONG_SHARD);
    return cached;
  }
  if (result.moving) {
    cached.set_code(proto::CommandResult::MOVING);
    return cached;
  }
  switch (result.txn) {
    case TxnOutcome::kLocked:
      cached.set_code(proto::CommandResult::LOCKED);
      if (result.lock.has_value()) {
        *cached.mutable_lock() = *result.lock;
      }
      return cached;
    case TxnOutcome::kValidationFailed:
      cached.set_code(proto::CommandResult::VALIDATION_FAILED);
      if (result.failed_check.has_value()) {
        cached.set_failed_check(*result.failed_check);
      }
      return cached;
    case TxnOutcome::kAborted:
      cached.set_code(proto::CommandResult::TXN_ABORTED);
      return cached;
    case TxnOutcome::kAlreadyCommitted:
      cached.set_code(proto::CommandResult::ALREADY_COMMITTED);
      return cached;
    case TxnOutcome::kNone:
      break;
  }
  cached.set_code(result.cas_mismatch ? proto::CommandResult::CAS_MISMATCH
                                      : proto::CommandResult::OK);
  if (result.value.has_value()) {
    cached.set_value(*result.value);
  }
  if (result.failed_check.has_value()) {
    cached.set_failed_check(*result.failed_check);
  }
  return cached;
}

}  // namespace

KvStateMachine::KvStateMachine(std::unique_ptr<lsm::DB> db) : db_(std::move(db)) {}

std::string KvStateMachine::UserKey(std::string_view key) {
  std::string out(1, kUserPrefix);
  out.append(key);
  return out;
}

std::string KvStateMachine::SessionKey(std::string_view client_id) {
  std::string out(1, kSessionPrefix);
  out.append(client_id);
  return out;
}

std::string KvStateMachine::MetaKey(std::string_view name) {
  std::string out(1, kMetaPrefix);
  out.append(name);
  return out;
}

Result<std::unique_ptr<KvStateMachine>> KvStateMachine::Open(lsm::Options options) {
  auto opened = lsm::DB::Open(std::move(options));
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<KvStateMachine> sm(new KvStateMachine(opened.TakeValue()));
  RAFTKV_RETURN_IF_ERROR(sm->LoadAppliedIndex());
  RAFTKV_RETURN_IF_ERROR(sm->LoadRange());
  RAFTKV_RETURN_IF_ERROR(sm->LoadLockCount());
  return sm;
}

Status KvStateMachine::LoadAppliedIndex() {
  auto got = db_->Get(MetaKey(kAppliedIndexKey));
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  const std::optional<std::string>& stored = *got;
  if (!stored.has_value()) {
    last_applied_index_ = 0;
    return Status::Ok();
  }
  const std::string& raw = *stored;
  if (raw.size() != 8) {
    return Status::Corruption("applied index is not 8 bytes");
  }
  last_applied_index_ = lsm::DecodeFixed64(raw.data());
  LOG_INFO("statemachine: recovered at applied index %llu",
           static_cast<unsigned long long>(last_applied_index_));
  return Status::Ok();
}

namespace {
// The lock a lookup found, or null. Callers test a pointer rather than reaching
// through a Result into an optional.
const proto::LockRecord* HeldLock(const Result<std::optional<proto::LockRecord>>& r) {
  if (!r.IsOk()) {
    return nullptr;
  }
  const std::optional<proto::LockRecord>& v = *r;
  return v.has_value() ? &v.value() : nullptr;
}
}  // namespace

std::string KvStateMachine::LockKey(std::string_view key) {
  std::string out(1, kLockPrefix);
  out.append(key);
  return out;
}

std::string KvStateMachine::TxnKey(std::string_view txid) {
  std::string out(1, kTxnPrefix);
  out.append(txid);
  return out;
}

Status KvStateMachine::LoadLockCount() {
  int64_t count = 0;
  const std::string lo(1, kLockPrefix);
  const std::string hi(1, static_cast<char>(kLockPrefix + 1));
  RAFTKV_RETURN_IF_ERROR(db_->Scan(lo, hi, 0, [&count](std::string_view, std::string_view) {
    ++count;
    return Status::Ok();
  }));
  lock_count_ = count;
  return Status::Ok();
}

Result<std::optional<proto::LockRecord>> KvStateMachine::GetLock(std::string_view key) const {
  auto got = db_->Get(LockKey(key));
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  const std::optional<std::string>& stored = *got;
  if (!stored.has_value()) {
    return std::optional<proto::LockRecord>{};
  }
  proto::LockRecord record;
  if (!record.ParseFromString(stored.value())) {
    return Status::Corruption("lock record failed to parse");
  }
  return std::optional<proto::LockRecord>{std::move(record)};
}

std::optional<proto::LockRecord> KvStateMachine::FindBlockingLock(std::string_view start,
                                                                  std::string_view end) const {
  const std::lock_guard<std::mutex> lock(mutex_);
  if (lock_count_ == 0) {
    return std::nullopt;
  }
  std::optional<proto::LockRecord> found;
  if (start == end) {
    auto got = GetLock(start);
    const proto::LockRecord* held = HeldLock(got);
    if (held != nullptr && held->kind() != proto::LockRecord::LOCK_ONLY) {
      found = *held;
    }
    return found;
  }
  const std::string lo = LockKey(start);
  std::string hi;
  if (end.empty()) {
    hi.push_back(static_cast<char>(kLockPrefix + 1));
  } else {
    hi = LockKey(end);
  }
  (void)db_->Scan(lo, hi, 0, [&](std::string_view, std::string_view value) {
    proto::LockRecord record;
    if (record.ParseFromString(std::string(value)) &&
        record.kind() != proto::LockRecord::LOCK_ONLY && !found.has_value()) {
      found = std::move(record);
    }
    return Status::Ok();
  });
  return found;
}

Result<std::optional<std::string>> KvStateMachine::TxnDecision(std::string_view txid) const {
  auto got = db_->Get(TxnKey(txid));
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  return *got;
}

Status KvStateMachine::LoadRange() {
  auto got = db_->Get(MetaKey("range"));
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  range_ = proto::RangeState();
  const std::optional<std::string>& stored = *got;
  if (stored.has_value() && !range_.ParseFromString(stored.value())) {
    return Status::Corruption("range state failed to parse");
  }
  return Status::Ok();
}

RangeVerdict KvStateMachine::JudgeKey(std::string_view key, bool write) const {
  if (!range_.configured()) {
    return RangeVerdict::kOk;
  }
  if (range_.owned_empty()) {
    return RangeVerdict::kWrongShard;
  }
  if (key < std::string_view(range_.owned_lo()) ||
      (!range_.owned_unbounded() && key >= std::string_view(range_.owned_hi()))) {
    return RangeVerdict::kWrongShard;
  }
  if (write && range_.fenced() && key >= std::string_view(range_.fence_lo()) &&
      (range_.fence_unbounded() || key < std::string_view(range_.fence_hi()))) {
    return RangeVerdict::kMoving;
  }
  return RangeVerdict::kOk;
}

RangeVerdict KvStateMachine::CheckRange(std::string_view start, std::string_view end,
                                        bool write) const {
  const std::lock_guard<std::mutex> lock(mutex_);
  if (start == end) {
    return JudgeKey(start, write);
  }
  if (!range_.configured()) {
    return RangeVerdict::kOk;
  }
  if (range_.owned_empty()) {
    return RangeVerdict::kWrongShard;
  }
  // [start, end) must sit entirely inside the owned range.
  const bool low_ok = start >= std::string_view(range_.owned_lo());
  const bool high_ok =
      range_.owned_unbounded() || (!end.empty() && end <= std::string_view(range_.owned_hi()));
  if (!low_ok || !high_ok) {
    return RangeVerdict::kWrongShard;
  }
  return RangeVerdict::kOk;
}

RangeVerdict KvStateMachine::JudgeCommand(const proto::Command& command) const {
  RangeVerdict worst = RangeVerdict::kOk;
  auto fold = [&worst](RangeVerdict v) {
    if (v == RangeVerdict::kWrongShard || (v == RangeVerdict::kMoving && worst == RangeVerdict::kOk)) {
      worst = v;
    }
  };
  switch (command.op_case()) {
    case proto::Command::kPut:
      fold(JudgeKey(command.put().key(), true));
      break;
    case proto::Command::kDelete:
      fold(JudgeKey(command.delete_().key(), true));
      break;
    case proto::Command::kCas:
      fold(JudgeKey(command.cas().key(), true));
      break;
    case proto::Command::kBatch:
      for (const auto& check : command.batch().checks()) {
        fold(JudgeKey(check.key(), true));
      }
      for (const auto& op : command.batch().ops()) {
        fold(JudgeKey(op.op_case() == proto::BatchOp::kPut ? op.put().key() : op.delete_().key(),
                      true));
      }
      break;
    default:
      break;
  }
  return worst;
}

ApplyResult KvStateMachine::CheckWriteLocks(const proto::Command& command, bool* blocked) const {
  ApplyResult result;
  *blocked = false;
  if (lock_count_ == 0) {
    return result;
  }
  std::vector<std::string_view> keys;
  switch (command.op_case()) {
    case proto::Command::kPut:
      keys.push_back(command.put().key());
      break;
    case proto::Command::kDelete:
      keys.push_back(command.delete_().key());
      break;
    case proto::Command::kCas:
      keys.push_back(command.cas().key());
      break;
    case proto::Command::kBatch:
      for (const auto& c : command.batch().checks()) keys.push_back(c.key());
      for (const auto& op : command.batch().ops()) {
        keys.push_back(op.op_case() == proto::BatchOp::kPut ? std::string_view(op.put().key())
                                                           : std::string_view(op.delete_().key()));
      }
      break;
    default:
      return result;
  }
  for (std::string_view key : keys) {
    auto lock = GetLock(key);
    if (!lock.IsOk()) {
      result.status = lock.GetStatus();
      *blocked = true;
      return result;
    }
    if (const proto::LockRecord* held = HeldLock(lock); held != nullptr) {
      result.status = Status::Ok();
      result.txn = TxnOutcome::kLocked;
      result.lock = *held;
      *blocked = true;
      return result;
    }
  }
  return result;
}

ApplyResult KvStateMachine::PrepareTxn(const proto::TxnCmd& cmd, lsm::WriteBatch* batch) {
  ApplyResult result;
  result.status = Status::Ok();
  const std::string& txid = cmd.txid();
  if (txid.empty()) {
    result.status = Status::InvalidArgument("a transaction needs an id");
    return result;
  }
  auto marker = db_->Get(TxnKey(txid));
  if (!marker.IsOk()) {
    result.status = marker.GetStatus();
    return result;
  }
  const std::optional<std::string>& decided = *marker;
  const bool committed = decided.has_value() && decided.value() == "C";
  const bool aborted = decided.has_value() && decided.value() == "A";

  switch (cmd.kind()) {
    case proto::TxnCmd::PREWRITE: {
      if (aborted || committed) {
        // A prewrite that arrives after its transaction was settled (a delayed
        // retry) must not resurrect a lock.
        result.txn = TxnOutcome::kAborted;
        return result;
      }
      if (cmd.writes_size() == 0) {
        result.status = Status::InvalidArgument("a prewrite needs at least one key");
        return result;
      }
      for (int i = 0; i < cmd.writes_size(); ++i) {
        const auto& w = cmd.writes(i);
        const RangeVerdict verdict = JudgeKey(w.key(), true);
        if (verdict != RangeVerdict::kOk) {
          result.wrong_shard = verdict == RangeVerdict::kWrongShard;
          result.moving = verdict == RangeVerdict::kMoving;
          return result;
        }
        auto lock = GetLock(w.key());
        if (!lock.IsOk()) {
          result.status = lock.GetStatus();
          return result;
        }
        if (const proto::LockRecord* held = HeldLock(lock);
            held != nullptr && held->txid() != txid) {
          result.txn = TxnOutcome::kLocked;
          result.lock = *held;
          return result;
        }
        if (w.validate()) {
          auto current = db_->Get(UserKey(w.key()));
          if (!current.IsOk()) {
            result.status = current.GetStatus();
            return result;
          }
          const std::optional<std::string>& observed = *current;
          const bool holds = w.expected_absent() ? !observed.has_value()
                                               : (observed.has_value() && observed.value() == w.expected());
          if (!holds) {
            result.txn = TxnOutcome::kValidationFailed;
            result.failed_check = static_cast<uint32_t>(i);
            return result;
          }
        }
      }
      // Everything passed: take every lock. All or nothing, in one write.
      int64_t added = 0;
      for (const auto& w : cmd.writes()) {
        auto existing = GetLock(w.key());
        if (existing.IsOk() && HeldLock(existing) == nullptr) {
          ++added;
        }
        proto::LockRecord record;
        record.set_txid(txid);
        record.set_primary_key(cmd.primary_key());
        record.set_kind(w.kind());
        record.set_value(w.value());
        batch->Put(LockKey(w.key()), record.SerializeAsString());
      }
      pending_lock_delta_ += added;
      return result;
    }

    case proto::TxnCmd::COMMIT: {
      if (cmd.primary() && aborted) {
        result.txn = TxnOutcome::kAborted;
        return result;
      }
      int64_t removed = 0;
      for (const auto& key : cmd.keys()) {
        auto lock = GetLock(key);
        if (!lock.IsOk()) {
          result.status = lock.GetStatus();
          return result;
        }
        if (const proto::LockRecord* held = HeldLock(lock);
            held != nullptr && held->txid() == txid) {
          const proto::LockRecord& record = *held;
          if (record.kind() == proto::LockRecord::PUT) {
            batch->Put(UserKey(key), record.value());
          } else if (record.kind() == proto::LockRecord::DELETE) {
            batch->Delete(UserKey(key));
          }
          batch->Delete(LockKey(key));
          ++removed;
        } else if (!committed) {
          // The lock is gone and no commit was recorded: it was rolled back.
          result.txn = TxnOutcome::kAborted;
          pending_lock_delta_ = 0;
          return result;
        }
        // else: already applied by an earlier attempt; committing is idempotent.
      }
      batch->Put(TxnKey(txid), "C");
      pending_lock_delta_ -= removed;
      return result;
    }

    case proto::TxnCmd::ROLLBACK: {
      if (committed) {
        result.txn = TxnOutcome::kAlreadyCommitted;
        return result;
      }
      int64_t removed = 0;
      for (const auto& key : cmd.keys()) {
        auto lock = GetLock(key);
        if (const proto::LockRecord* held = HeldLock(lock);
            held != nullptr && held->txid() == txid) {
          batch->Delete(LockKey(key));
          ++removed;
        }
      }
      // The marker is what stops a late prewrite of this transaction from
      // taking a lock after we have settled it.
      batch->Put(TxnKey(txid), "A");
      pending_lock_delta_ -= removed;
      return result;
    }

    default:
      break;
  }
  result.status = Status::InvalidArgument("unknown transaction command");
  return result;
}

ApplyResult KvStateMachine::PrepareRange(const proto::RangeCmd& cmd, lsm::WriteBatch* batch) {
  ApplyResult result;
  if (cmd.epoch() < range_.epoch()) {
    result.status = Status::InvalidArgument("stale shard epoch " + std::to_string(cmd.epoch()) +
                                            " (already at " + std::to_string(range_.epoch()) + ")");
    return result;
  }
  if (cmd.kind() == proto::RangeCmd::PURGE) {
    // Only keys this group has given away may be purged. Refusing anything that
    // overlaps the owned range is what makes it impossible to wipe live data
    // with a mistaken or stale command.
    if (range_.configured() && !range_.owned_empty()) {
      const bool below_owned = !cmd.unbounded_hi() && cmd.hi() <= range_.owned_lo();
      const bool above_owned = !range_.owned_unbounded() && cmd.lo() >= range_.owned_hi();
      if (!below_owned && !above_owned) {
        result.status = Status::InvalidArgument("purge range overlaps the owned range");
        return result;
      }
    }
    const std::string lo = UserKey(cmd.lo());
    std::string hi;
    if (cmd.unbounded_hi()) {
      hi.push_back(static_cast<char>(kUserPrefix + 1));
    } else {
      hi = UserKey(cmd.hi());
    }
    const size_t limit = cmd.limit() == 0 ? 10000 : cmd.limit();
    size_t deleted = 0;
    const Status scanned = db_->Scan(lo, hi, limit, [&](std::string_view key, std::string_view) {
      batch->Delete(key);
      ++deleted;
      return Status::Ok();
    });
    if (!scanned.IsOk()) {
      result.status = scanned;
      return result;
    }
    result.value = std::to_string(deleted);
    result.status = Status::Ok();
    return result;
  }

  proto::RangeState next = range_;
  next.set_configured(true);
  next.set_epoch(cmd.epoch());
  if (cmd.kind() == proto::RangeCmd::SET_OWNED) {
    next.set_owned_empty(cmd.empty());
    next.set_owned_lo(cmd.lo());
    next.set_owned_hi(cmd.hi());
    next.set_owned_unbounded(cmd.unbounded_hi());
    next.set_fenced(false);  // the handover this protected has finished
  } else {
    if (range_.configured() && range_.owned_empty()) {
      result.status = Status::InvalidArgument("cannot fence a range this group does not own");
      return result;
    }
    if (!range_.configured()) {
      // A group nobody ever assigned a range owns the whole key space. Make
      // that explicit now, so the fence has something to be inside of.
      next.set_owned_lo("");
      next.set_owned_unbounded(true);
    }
    // Locks live beside the data but are not copied with it, so a range with a
    // transaction in flight cannot move until that transaction settles.
    if (lock_count_ > 0) {
      std::string hi;
      if (cmd.unbounded_hi()) {
        hi.push_back(static_cast<char>(kLockPrefix + 1));
      } else {
        hi = LockKey(cmd.hi());
      }
      bool has_lock = false;
      (void)db_->Scan(LockKey(cmd.lo()), hi, 1, [&](std::string_view, std::string_view) {
        has_lock = true;
        return Status::Ok();
      });
      if (has_lock) {
        result.status = Status::Unavailable("range has transactions in flight; retry after they settle");
        return result;
      }
    }
    next.set_fenced(true);
    next.set_fence_lo(cmd.lo());
    next.set_fence_hi(cmd.hi());
    next.set_fence_unbounded(cmd.unbounded_hi());
  }
  batch->Put(MetaKey("range"), next.SerializeAsString());
  pending_range_ = std::move(next);
  result.status = Status::Ok();
  return result;
}

uint64_t KvStateMachine::LastAppliedIndex() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return last_applied_index_;
}

Status KvStateMachine::Sync() { return db_->SyncWal(); }

Result<std::optional<std::string>> KvStateMachine::Get(std::string_view key) const {
  return db_->Get(UserKey(key));
}

Status KvStateMachine::Scan(
    std::string_view start, std::string_view end, size_t limit,
    const std::function<Status(std::string_view, std::string_view)>& fn) const {
  // User keys live under one prefix byte, so the scan stays inside that
  // namespace and never surfaces metadata or session rows. An unbounded end
  // becomes the first key past the whole namespace.
  const std::string lo = UserKey(start);
  std::string hi;
  if (end.empty()) {
    hi.push_back(static_cast<char>(kUserPrefix + 1));
  } else {
    hi = UserKey(end);
  }
  return db_->Scan(lo, hi, limit, [&fn](std::string_view key, std::string_view value) {
    return fn(key.substr(1), value);
  });
}

Result<std::optional<proto::SessionEntry>> KvStateMachine::LookupSession(
    const std::string& client_id) const {
  auto got = db_->Get(SessionKey(client_id));
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  const std::optional<std::string>& stored = *got;
  if (!stored.has_value()) {
    return std::optional<proto::SessionEntry>{};
  }
  proto::SessionEntry entry;
  if (!entry.ParseFromString(*stored)) {
    return Status::Corruption("session entry failed to parse");
  }
  return std::optional<proto::SessionEntry>{std::move(entry)};
}

ApplyResult KvStateMachine::Prepare(const proto::Command& command, lsm::WriteBatch* batch) {
  ApplyResult result;
  pending_lock_delta_ = 0;

  // Ownership is decided here, inside the replicated apply, so every replica
  // accepts or refuses exactly the same writes either side of a handover.
  const RangeVerdict verdict = JudgeCommand(command);
  if (verdict != RangeVerdict::kOk) {
    result.status = Status::Ok();
    result.wrong_shard = verdict == RangeVerdict::kWrongShard;
    result.moving = verdict == RangeVerdict::kMoving;
    return result;
  }

  // A plain write to a key an in-flight transaction holds would be silently
  // overwritten when that transaction commits, so it is refused until the lock
  // is settled.
  bool blocked = false;
  ApplyResult locked = CheckWriteLocks(command, &blocked);
  if (blocked) {
    return locked;
  }

  switch (command.op_case()) {
    case proto::Command::kPut:
      batch->Put(UserKey(command.put().key()), command.put().value());
      result.status = Status::Ok();
      return result;

    case proto::Command::kDelete:
      batch->Delete(UserKey(command.delete_().key()));
      result.status = Status::Ok();
      return result;

    case proto::Command::kCas: {
      const auto& cas = command.cas();
      auto current = db_->Get(UserKey(cas.key()));
      if (!current.IsOk()) {
        result.status = current.GetStatus();
        return result;
      }

      const std::optional<std::string>& observed = *current;
      const bool exists = observed.has_value();
      const bool matches = cas.has_expected() ? (exists && *observed == cas.expected()) : !exists;
      if (!matches) {
        // A failed comparison is a successful apply that reports a negative
        // answer, not an error. Conflating the two would make the client retry
        // a command that did exactly what it was asked to do.
        result.status = Status::Ok();
        result.cas_mismatch = true;
        if (exists) {
          result.value = *observed;
        }
        return result;
      }
      batch->Put(UserKey(cas.key()), cas.new_value());
      result.status = Status::Ok();
      return result;
    }

    case proto::Command::kBatch: {
      const auto& cmd = command.batch();
      if (cmd.ops_size() == 0 && cmd.checks_size() == 0) {
        result.status = Status::InvalidArgument("an empty batch does nothing");
        return result;
      }
      // Checks see the state as it was before this batch, never its own
      // writes, so the outcome does not depend on the order of ops.
      for (int i = 0; i < cmd.checks_size(); ++i) {
        const auto& check = cmd.checks(i);
        auto current = db_->Get(UserKey(check.key()));
        if (!current.IsOk()) {
          result.status = current.GetStatus();
          return result;
        }
        const std::optional<std::string>& observed = *current;
        const bool holds = check.has_expected()
                               ? (observed.has_value() && *observed == check.expected())
                               : !observed.has_value();
        if (!holds) {
          result.status = Status::Ok();
          result.cas_mismatch = true;
          result.failed_check = static_cast<uint32_t>(i);
          return result;
        }
      }
      for (const auto& op : cmd.ops()) {
        if (op.op_case() == proto::BatchOp::kPut) {
          batch->Put(UserKey(op.put().key()), op.put().value());
        } else if (op.op_case() == proto::BatchOp::kDelete) {
          batch->Delete(UserKey(op.delete_().key()));
        } else {
          result.status = Status::InvalidArgument("batch operation has no kind");
          return result;
        }
      }
      result.status = Status::Ok();
      return result;
    }

    case proto::Command::kRange:
      return PrepareRange(command.range(), batch);

    case proto::Command::kTxn:
      return PrepareTxn(command.txn(), batch);

    case proto::Command::kImport:
      for (const auto& put : command.import().puts()) {
        batch->Put(UserKey(put.key()), put.value());
      }
      result.status = Status::Ok();
      return result;

    case proto::Command::OP_NOT_SET:
    default:
      result.status = Status::InvalidArgument("command has no operation set");
      return result;
  }
}

ApplyResult KvStateMachine::AdvanceOnly(uint64_t index, ApplyResult result) {
  lsm::WriteBatch batch;
  std::string encoded_index;
  lsm::PutFixed64(&encoded_index, index);
  batch.Put(MetaKey(kAppliedIndexKey), encoded_index);
  const Status s = db_->Write(batch);
  if (!s.IsOk()) {
    result.status = s;
    return result;
  }
  last_applied_index_ = index;
  return result;
}

ApplyResult KvStateMachine::Apply(uint64_t index, std::span<const std::byte> command) {
  const std::lock_guard<std::mutex> lock(mutex_);

  ApplyResult result;

  // Recovery replays the tail of the Raft log, so an index at or below what is
  // already applied must be ignored rather than applied a second time.
  if (index <= last_applied_index_) {
    result.status = Status::Ok();
    result.deduplicated = true;
    return result;
  }

  const uint64_t digest = Digest(command);

  proto::Command parsed;
  if (!parsed.ParseFromArray(command.data(), static_cast<int>(command.size()))) {
    // A malformed command still advances the applied index. Every replica sees
    // the same bytes and rejects them identically, so this is deterministic --
    // and refusing to advance would wedge the log on a poison entry forever.
    result.status = Status::InvalidArgument("command failed to parse");
    lsm::WriteBatch batch;
    std::string encoded_index;
    lsm::PutFixed64(&encoded_index, index);
    batch.Put(MetaKey(kAppliedIndexKey), encoded_index);
    const Status s = db_->Write(batch);
    if (!s.IsOk()) {
      result.status = s;
      return result;
    }
    last_applied_index_ = index;
    return result;
  }

  // Exactly-once. A repeat of the most recent sequence returns the cached
  // answer; an older sequence cannot be answered because only the latest
  // result is retained.
  std::optional<proto::SessionEntry> session;
  if (!parsed.client_id().empty()) {
    auto looked_up = LookupSession(parsed.client_id());
    if (!looked_up.IsOk()) {
      result.status = looked_up.GetStatus();
      return result;
    }
    session = looked_up.TakeValue();

    if (session.has_value()) {
      // Each of these paths consumes a log entry without changing user data.
      // The applied index must still move past it: left behind, the state
      // machine would trail Raft's log and refuse a snapshot taken at Raft's
      // index. Writing only the index key is cheap and deterministic.
      if (parsed.seq() == session->last_seq()) {
        const bool same_command =
            session->command_digest() == 0 || session->command_digest() == digest;
        if (!same_command) {
          result.status = Status::InvalidArgument(
              "sequence " + std::to_string(parsed.seq()) + " for client '" + parsed.client_id() +
              "' was already used by a different command; a client that loses its sequence "
              "counter must use a new client_id");
          return AdvanceOnly(index, std::move(result));
        }
        ApplyResult cached = ResultFromCached(session->last_result());
        return AdvanceOnly(index, std::move(cached));
      }
      if (parsed.seq() < session->last_seq()) {
        result.status = Status::InvalidArgument(
            "sequence " + std::to_string(parsed.seq()) + " is older than the cached " +
            std::to_string(session->last_seq()) + "; result no longer available");
        return AdvanceOnly(index, std::move(result));
      }
    }
  }

  lsm::WriteBatch batch;
  result = Prepare(parsed, &batch);

  // The effect, the session entry, and the applied index all go into this one
  // batch. One WAL record, one CRC: a crash leaves all three or none.
  if (!parsed.client_id().empty()) {
    proto::SessionEntry entry;
    entry.set_last_seq(parsed.seq());
    *entry.mutable_last_result() = CacheableResult(result);
    entry.set_last_touch_index(index);
    entry.set_command_digest(digest);
    batch.Put(SessionKey(parsed.client_id()), entry.SerializeAsString());
  }

  std::string encoded_index;
  lsm::PutFixed64(&encoded_index, index);
  batch.Put(MetaKey(kAppliedIndexKey), encoded_index);

  const Status s = db_->Write(batch);
  if (!s.IsOk()) {
    pending_range_.reset();
    pending_lock_delta_ = 0;
    result.status = s;
    return result;
  }
  last_applied_index_ = index;
  if (pending_range_.has_value()) {
    range_ = std::move(*pending_range_);
    pending_range_.reset();
  }
  lock_count_ += pending_lock_delta_;
  pending_lock_delta_ = 0;
  return result;
}

namespace {

// Snapshot wire format:
//   length-prefixed SnapshotHeader
//   repeated { length-prefixed key | length-prefixed value }
//
// Keys are stored with their namespace tag intact, so the session table and
// the applied index travel with the user data automatically. Dropping the
// session table would silently break exactly-once after a restore: a client
// retry would look like a new command and be applied twice.
class MemorySnapshot : public Snapshot {
 public:
  MemorySnapshot(uint64_t index, uint64_t term, std::string bytes)
      : index_(index), term_(term), bytes_(std::move(bytes)) {}

  uint64_t LastIncludedIndex() const override { return index_; }
  uint64_t LastIncludedTerm() const override { return term_; }
  uint64_t Size() const override { return bytes_.size(); }

  Result<std::string> ReadChunk(uint64_t offset, size_t max_bytes) const override {
    if (offset > bytes_.size()) {
      return Status::InvalidArgument("snapshot chunk offset past end");
    }
    const size_t available = bytes_.size() - offset;
    return bytes_.substr(offset, std::min<size_t>(max_bytes, available));
  }

 private:
  uint64_t index_;
  uint64_t term_;
  std::string bytes_;
};

}  // namespace

Result<std::unique_ptr<Snapshot>> KvStateMachine::TakeSnapshot(uint64_t index, uint64_t term) {
  const std::lock_guard<std::mutex> lock(mutex_);

  // A snapshot must describe a state the machine actually reached. Producing
  // one for an index the machine has not applied would let Raft compact its
  // log past data the snapshot does not contain.
  if (index != last_applied_index_) {
    return Status::InvalidArgument("snapshot requested at index " + std::to_string(index) +
                                   " but applied index is " + std::to_string(last_applied_index_));
  }

  std::string out;
  proto::SnapshotHeader header;
  header.set_last_included_index(index);
  header.set_last_included_term(term);
  lsm::PutLengthPrefixed(&out, header.SerializeAsString());

  const Status s = db_->ScanAll([&out](std::string_view key, std::string_view value) {
    lsm::PutLengthPrefixed(&out, key);
    lsm::PutLengthPrefixed(&out, value);
    return Status::Ok();
  });
  if (!s.IsOk()) {
    return s;
  }

  std::unique_ptr<Snapshot> snapshot =
      std::make_unique<MemorySnapshot>(index, term, std::move(out));
  return snapshot;
}

Status KvStateMachine::RestoreSnapshot(Snapshot& snapshot) {
  const std::lock_guard<std::mutex> lock(mutex_);

  // Materialize the whole snapshot before touching any state. Restoring
  // incrementally would leave the machine in a half-replaced state if a chunk
  // read failed partway through, and there would be nothing to roll back to.
  std::string bytes;
  bytes.reserve(snapshot.Size());
  constexpr size_t kChunk = size_t{1} * 1024 * 1024;
  while (bytes.size() < snapshot.Size()) {
    auto chunk = snapshot.ReadChunk(bytes.size(), kChunk);
    if (!chunk.IsOk()) {
      return chunk.GetStatus();
    }
    if (chunk->empty()) {
      return Status::Corruption("snapshot ended before its declared size");
    }
    bytes.append(*chunk);
  }

  std::string_view input(bytes);
  std::string_view header_bytes;
  if (!lsm::GetLengthPrefixed(&input, &header_bytes)) {
    return Status::Corruption("snapshot: truncated header");
  }
  proto::SnapshotHeader header;
  if (!header.ParseFromArray(header_bytes.data(), static_cast<int>(header_bytes.size()))) {
    return Status::Corruption("snapshot: header failed to parse");
  }

  // Wholesale replacement, not a merge. Merging would keep keys the snapshot's
  // author had deleted, so the restored replica would diverge from the one it
  // copied.
  RAFTKV_RETURN_IF_ERROR(db_->DestroyContents());

  lsm::WriteBatch batch;
  size_t batched = 0;
  while (!input.empty()) {
    std::string_view key;
    std::string_view value;
    if (!lsm::GetLengthPrefixed(&input, &key) || !lsm::GetLengthPrefixed(&input, &value)) {
      return Status::Corruption("snapshot: truncated record");
    }
    batch.Put(key, value);
    if (++batched >= 1000) {
      RAFTKV_RETURN_IF_ERROR(db_->Write(batch));
      batch.Clear();
      batched = 0;
    }
  }
  if (!batch.Empty()) {
    RAFTKV_RETURN_IF_ERROR(db_->Write(batch));
  }

  // The applied index travels inside the snapshot body as a normal key, but
  // write it explicitly too: a snapshot taken at index 0 of an empty machine
  // has no such key, and the in-memory value must still be correct.
  {
    lsm::WriteBatch meta;
    std::string encoded;
    lsm::PutFixed64(&encoded, header.last_included_index());
    meta.Put(MetaKey(kAppliedIndexKey), encoded);
    RAFTKV_RETURN_IF_ERROR(db_->Write(meta));
  }

  RAFTKV_RETURN_IF_ERROR(db_->SyncWal());
  last_applied_index_ = header.last_included_index();
  RAFTKV_RETURN_IF_ERROR(LoadRange());
  RAFTKV_RETURN_IF_ERROR(LoadLockCount());

  LOG_INFO("statemachine: restored snapshot at index %llu term %llu",
           static_cast<unsigned long long>(header.last_included_index()),
           static_cast<unsigned long long>(header.last_included_term()));
  return Status::Ok();
}

}  // namespace raftkv::statemachine
