#include "lsm/db.h"

#include <algorithm>
#include <map>
#include <utility>

#include "lsm/coding.h"

#include "common/log.h"

namespace raftkv::lsm {
namespace {

constexpr const char* kWalName = "LOG.wal";

// Applies a replayed batch into a memtable and tracks the highest sequence
// seen, which is where recovery learns where to resume numbering.
class MemTableInserter : public WriteBatch::Handler {
 public:
  MemTableInserter(MemTable* mem, SequenceNumber* max_seq) : mem_(mem), max_seq_(max_seq) {}

  void Put(SequenceNumber seq, std::string_view key, std::string_view value) override {
    mem_->Add(seq, ValueType::kValue, key, value);
    *max_seq_ = std::max(*max_seq_, seq);
  }
  void Delete(SequenceNumber seq, std::string_view key) override {
    mem_->Add(seq, ValueType::kDeletion, key, "");
    *max_seq_ = std::max(*max_seq_, seq);
  }

 private:
  MemTable* mem_;
  SequenceNumber* max_seq_;
};

bool IsTableFile(const std::filesystem::path& path, uint64_t* number) {
  if (path.extension() != ".sst") {
    return false;
  }
  const std::string stem = path.stem().string();
  if (stem.empty()) {
    return false;
  }
  try {
    *number = std::stoull(stem);
  } catch (...) {
    return false;
  }
  return true;
}

}  // namespace

DB::DB(Options options) : options_(std::move(options)) {}

DB::~DB() {
  if (wal_ != nullptr) {
    (void)wal_->Close();
  }
}

std::filesystem::path DB::WalPath() const { return options_.dir / kWalName; }

std::filesystem::path DB::TablePath(uint64_t number) const {
  char name[32];
  std::snprintf(name, sizeof(name), "%010llu.sst", static_cast<unsigned long long>(number));
  return options_.dir / name;
}

Result<std::unique_ptr<DB>> DB::Open(Options options) {
  if (options.dir.empty()) {
    return Status::InvalidArgument("Options::dir is required");
  }

  std::error_code ec;
  std::filesystem::create_directories(options.dir, ec);
  if (ec) {
    return Status::IoError("create " + options.dir.string() + ": " + ec.message());
  }

  std::unique_ptr<DB> db(new DB(std::move(options)));
  RAFTKV_RETURN_IF_ERROR(db->Recover());
  return db;
}

Status DB::LoadTables() {
  std::vector<uint64_t> numbers;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(options_.dir, ec)) {
    uint64_t number = 0;
    if (IsTableFile(entry.path(), &number)) {
      numbers.push_back(number);
    }
  }
  if (ec) {
    return Status::IoError("scanning " + options_.dir.string() + ": " + ec.message());
  }

  // Higher file numbers are newer, so the read path visits them first.
  std::sort(numbers.begin(), numbers.end(), std::greater<>());

  for (uint64_t number : numbers) {
    auto opened = SsTable::Open(TablePath(number));
    if (!opened.IsOk()) {
      return opened.GetStatus();
    }
    auto table = opened.TakeValue();
    // Sequence numbers must resume above everything already on disk, or reads
    // run at a snapshot that predates the flushed data and see nothing.
    last_sequence_ = std::max(last_sequence_, table->MaxSequence());
    l0_.push_back(std::move(table));
    next_file_number_ = std::max(next_file_number_, number + 1);
  }
  return Status::Ok();
}

Status DB::Recover() {
  mem_ = std::make_unique<MemTable>();

  RAFTKV_RETURN_IF_ERROR(LoadTables());

  // Replay the WAL, truncating any record damaged by a crash mid-append. Those
  // records were never acknowledged as durable, so dropping them loses nothing
  // a caller was promised -- and it drops the effect and its applied index
  // together, because they share a record.
  std::vector<std::string> records;
  RAFTKV_RETURN_IF_ERROR(ReplayWal(WalPath(), /*truncate_partial=*/true, &records));

  SequenceNumber max_seq = 0;
  for (std::string& raw : records) {
    WriteBatch batch;
    RAFTKV_RETURN_IF_ERROR(WriteBatch::FromContents(std::move(raw), &batch));
    MemTableInserter inserter(mem_.get(), &max_seq);
    RAFTKV_RETURN_IF_ERROR(batch.Iterate(&inserter));
  }
  last_sequence_ = std::max(last_sequence_, max_seq);

  auto opened = WalWriter::Open(WalPath());
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  wal_ = opened.TakeValue();

  LOG_INFO("lsm: recovered %zu wal records, %zu tables, last sequence %llu", records.size(),
           l0_.size(), static_cast<unsigned long long>(last_sequence_));
  return Status::Ok();
}

Status DB::Write(WriteBatch& batch, const WriteOptions& options) {
  if (batch.Empty()) {
    return Status::Ok();
  }

  // Scoped deliberately. FlushMemTable and CompactRange each acquire
  // write_mutex_ themselves, so holding it across MaybeFlush() below would
  // deadlock on a non-recursive mutex -- and the symptom is a hung write, not
  // an error.
  {
    const std::lock_guard<std::mutex> writer_lock(write_mutex_);

    SequenceNumber first;
    {
      const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
      first = last_sequence_ + 1;
    }
    batch.SetSequence(first);

    // WAL first, memtable second. The reverse order would make a write visible
    // to readers before it is recoverable, so a crash could lose a value that a
    // concurrent reader had already observed.
    RAFTKV_RETURN_IF_ERROR(wal_->Append(batch.Contents()));

    const bool sync = options.sync.value_or(options_.sync_on_write);
    if (sync) {
      RAFTKV_RETURN_IF_ERROR(wal_->Sync());
    }

    {
      const std::lock_guard<std::shared_mutex> write_state(state_mutex_);
      SequenceNumber max_seq = last_sequence_;
      MemTableInserter inserter(mem_.get(), &max_seq);
      RAFTKV_RETURN_IF_ERROR(batch.Iterate(&inserter));
      last_sequence_ = std::max(last_sequence_, max_seq);
    }
  }

  return MaybeFlush();
}

Status DB::Put(std::string_view key, std::string_view value, const WriteOptions& options) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(batch, options);
}

Status DB::Delete(std::string_view key, const WriteOptions& options) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(batch, options);
}

Result<std::optional<std::string>> DB::Get(std::string_view key, const Snapshot* snapshot) const {
  // Copy the handles under the lock, then read without it. SSTables are
  // immutable and refcounted, so a compaction retiring a file mid-read cannot
  // invalidate what we are scanning.
  SequenceNumber sequence;
  std::vector<std::shared_ptr<SsTable>> l0;
  std::shared_ptr<SsTable> l1;
  std::string mem_value;
  std::optional<ValueType> mem_type;

  {
    const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
    sequence = snapshot != nullptr ? snapshot->Sequence() : last_sequence_;
    mem_type = mem_->Get(key, sequence, &mem_value);
    l0 = l0_;
    l1 = l1_;
  }

  // The memtable is the newest source; a tombstone here ends the search.
  if (mem_type.has_value()) {
    if (*mem_type == ValueType::kDeletion) {
      return std::optional<std::string>{};
    }
    return std::optional<std::string>{std::move(mem_value)};
  }

  auto search = [&](const std::shared_ptr<SsTable>& table,
                    std::optional<std::optional<std::string>>* out) -> Status {
    std::string value;
    auto got = table->Get(key, sequence, &value);
    if (!got.IsOk()) {
      return got.GetStatus();
    }
    if (!got->has_value()) {
      return Status::Ok();  // not in this table; keep looking
    }
    if (**got == ValueType::kDeletion) {
      *out = std::optional<std::string>{};
    } else {
      *out = std::optional<std::string>{std::move(value)};
    }
    return Status::Ok();
  };

  std::optional<std::optional<std::string>> answer;
  for (const auto& table : l0) {
    const Status s = search(table, &answer);
    if (!s.IsOk()) {
      return s;
    }
    if (answer.has_value()) {
      return *answer;
    }
  }
  if (l1 != nullptr) {
    const Status s = search(l1, &answer);
    if (!s.IsOk()) {
      return s;
    }
    if (answer.has_value()) {
      return *answer;
    }
  }

  return std::optional<std::string>{};
}

std::shared_ptr<Snapshot> DB::TakeSnapshot() {
  const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
  std::vector<std::shared_ptr<SsTable>> pinned = l0_;
  if (l1_ != nullptr) {
    pinned.push_back(l1_);
  }
  return std::shared_ptr<Snapshot>(new Snapshot(last_sequence_, std::move(pinned)));
}

Status DB::SyncWal() {
  const std::lock_guard<std::mutex> writer_lock(write_mutex_);
  return wal_->Sync();
}

SequenceNumber DB::LastSequence() const {
  const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
  return last_sequence_;
}

size_t DB::NumL0Tables() const {
  const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
  return l0_.size();
}

bool DB::HasL1Table() const {
  const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
  return l1_ != nullptr;
}

Status DB::MaybeFlush() {
  bool needs_flush = false;
  {
    const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
    needs_flush = mem_->ApproximateMemoryUsage() >= options_.memtable_bytes;
  }
  if (!needs_flush) {
    return Status::Ok();
  }
  RAFTKV_RETURN_IF_ERROR(FlushMemTable());

  size_t l0_count = 0;
  {
    const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
    l0_count = l0_.size();
  }
  if (l0_count >= options_.l0_compaction_trigger) {
    return CompactRange();
  }
  return Status::Ok();
}

Status DB::WriteLevel0Table(const MemTable& table, const std::filesystem::path& path) {
  auto created = SsTableBuilder::Create(path);
  if (!created.IsOk()) {
    return created.GetStatus();
  }
  auto builder = created.TakeValue();

  // std::map iterates in comparator order, which is exactly the ascending
  // internal-key order the builder requires.
  for (const auto& [internal_key, value] : table.Entries()) {
    RAFTKV_RETURN_IF_ERROR(builder->Add(internal_key, value));
  }
  return builder->Finish();
}

Status DB::FlushMemTable() {
  // Held for the whole flush so no writer can append to the memtable being
  // serialized. This is the simple correct thing; a background flush with an
  // immutable memtable is the Phase 9 optimization, and it should be made
  // against a profile rather than on principle.
  const std::lock_guard<std::mutex> writer_lock(write_mutex_);

  if (mem_->Empty()) {
    return Status::Ok();
  }

  const uint64_t number = next_file_number_++;
  const std::filesystem::path path = TablePath(number);
  RAFTKV_RETURN_IF_ERROR(WriteLevel0Table(*mem_, path));

  auto opened = SsTable::Open(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }

  // The table's name is only durable once the directory entry is. Without
  // this, a crash can leave a fully-synced file that its directory does not
  // list, and the data is gone despite every fsync having succeeded.
  RAFTKV_RETURN_IF_ERROR(SyncDirectory(options_.dir));

  {
    const std::lock_guard<std::shared_mutex> write_state(state_mutex_);
    l0_.insert(l0_.begin(), opened.TakeValue());
    mem_ = std::make_unique<MemTable>();
  }

  // The memtable's contents are now in a synced table, so the WAL that
  // described them is redundant. Truncating it is what keeps recovery bounded.
  RAFTKV_RETURN_IF_ERROR(wal_->Close());
  std::error_code ec;
  std::filesystem::remove(WalPath(), ec);
  if (ec) {
    return Status::IoError("removing stale wal: " + ec.message());
  }
  auto reopened = WalWriter::Open(WalPath());
  if (!reopened.IsOk()) {
    return reopened.GetStatus();
  }
  wal_ = reopened.TakeValue();

  LOG_DEBUG("lsm: flushed memtable to %s", path.string().c_str());
  return Status::Ok();
}

Status DB::CompactRange() {
  const std::lock_guard<std::mutex> writer_lock(write_mutex_);

  std::vector<std::shared_ptr<SsTable>> inputs;
  {
    const std::shared_lock<std::shared_mutex> read_state(state_mutex_);
    inputs = l0_;
    if (l1_ != nullptr) {
      inputs.push_back(l1_);
    }
  }
  if (inputs.size() < 2) {
    return Status::Ok();
  }

  // Merge every input into one sorted stream.
  //
  // Inputs arrive newest-first, and duplicates of the same internal key cannot
  // occur across them because sequence numbers are unique per mutation. So a
  // straight sorted merge is sufficient; there is no "newer wins" tie-break to
  // get wrong.
  std::map<std::string, std::string, InternalKeyComparator> merged;
  for (const auto& table : inputs) {
    auto it = table->NewIterator();
    while (true) {
      std::string_view key;
      std::string_view value;
      const Status s = it->Next(&key, &value);
      if (s.ErrCode() == Code::kNotFound) {
        break;
      }
      if (!s.IsOk()) {
        return s;
      }
      merged.emplace(std::string(key), std::string(value));
    }
  }

  const uint64_t number = next_file_number_++;
  const std::filesystem::path path = TablePath(number);

  auto created = SsTableBuilder::Create(path);
  if (!created.IsOk()) {
    return created.GetStatus();
  }
  auto builder = created.TakeValue();
  for (const auto& [key, value] : merged) {
    RAFTKV_RETURN_IF_ERROR(builder->Add(key, value));
  }
  RAFTKV_RETURN_IF_ERROR(builder->Finish());

  auto opened = SsTable::Open(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  RAFTKV_RETURN_IF_ERROR(SyncDirectory(options_.dir));

  std::vector<std::shared_ptr<SsTable>> retired;
  {
    const std::lock_guard<std::shared_mutex> write_state(state_mutex_);
    retired = std::move(l0_);
    l0_.clear();
    if (l1_ != nullptr) {
      retired.push_back(l1_);
    }
    l1_ = opened.TakeValue();
  }

  // Delete a retired file only when nothing else holds it. A live Snapshot
  // keeps its own shared_ptr, so use_count() > 1 means a reader would lose the
  // file out from under it.
  for (const auto& table : retired) {
    if (table.use_count() > 1) {
      LOG_DEBUG("lsm: %s still pinned by a snapshot; deferring delete",
                table->Path().string().c_str());
      continue;
    }
    const std::filesystem::path victim = table->Path();
    std::error_code ec;
    std::filesystem::remove(victim, ec);
  }

  LOG_DEBUG("lsm: compacted %zu tables into %s", inputs.size(), path.string().c_str());
  return Status::Ok();
}

}  // namespace raftkv::lsm
