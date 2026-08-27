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
      break;
    case proto::CommandResult::NOT_FOUND:
      out.status = Status::Ok();
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

proto::CommandResult CacheableResult(const ApplyResult& result) {
  proto::CommandResult cached;
  if (!result.status.IsOk()) {
    cached.set_code(proto::CommandResult::INVALID);
    return cached;
  }
  cached.set_code(result.cas_mismatch ? proto::CommandResult::CAS_MISMATCH
                                      : proto::CommandResult::OK);
  if (result.value.has_value()) {
    cached.set_value(*result.value);
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

uint64_t KvStateMachine::LastAppliedIndex() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return last_applied_index_;
}

Status KvStateMachine::Sync() { return db_->SyncWal(); }

Result<std::optional<std::string>> KvStateMachine::Get(std::string_view key) const {
  return db_->Get(UserKey(key));
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

    case proto::Command::OP_NOT_SET:
    default:
      result.status = Status::InvalidArgument("command has no operation set");
      return result;
  }
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
      if (parsed.seq() == session->last_seq()) {
        return ResultFromCached(session->last_result());
      }
      if (parsed.seq() < session->last_seq()) {
        result.status = Status::InvalidArgument(
            "sequence " + std::to_string(parsed.seq()) + " is older than the cached " +
            std::to_string(session->last_seq()) + "; result no longer available");
        return result;
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
    batch.Put(SessionKey(parsed.client_id()), entry.SerializeAsString());
  }

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

  LOG_INFO("statemachine: restored snapshot at index %llu term %llu",
           static_cast<unsigned long long>(header.last_included_index()),
           static_cast<unsigned long long>(header.last_included_term()));
  return Status::Ok();
}

}  // namespace raftkv::statemachine
