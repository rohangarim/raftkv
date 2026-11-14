#include "engine/lsmtree_engine.h"

#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <shared_mutex>
#include <utility>

// lsmtree's own headers. Included by their `lsm/` prefix, which is why this file
// is the only one that sees them and why raftkv's built-in engine lives under
// minilsm/ rather than lsm/.
#include "lsm/db.h"
#include "lsm/options.h"
#include "lsm/write_batch.h"

namespace raftkv::engine {
namespace {

// lsmtree's types live in the global ::lsm namespace; raftkv's `lsm::Status` is
// raftkv::lsm. Spelling the former out keeps the two from ever being confused.
namespace lt = ::lsm;

Status Convert(const lt::Status& s) {
  if (s.ok()) {
    return Status::Ok();
  }
  const std::string text = s.ToString();
  switch (s.code()) {
    case lt::Status::Code::kNotFound:
      return Status::NotFound(text);
    case lt::Status::Code::kCorruption:
      return Status::Corruption(text);
    case lt::Status::Code::kInvalidArgument:
      return Status::InvalidArgument(text);
    case lt::Status::Code::kNotSupported:
      return Status::NotSupported(text);
    default:
      return Status::IoError(text);
  }
}

lt::Options ToLsmTreeOptions(const Options& options) {
  lt::Options o;
  o.create_if_missing = true;
  o.memtable_size_bytes = options.memtable_bytes;
  o.l0_compaction_trigger = options.l0_compaction_trigger;
  // kEveryWrite is lsmtree's default and would be a second fsync per apply. The
  // Raft log already made the entry durable, so the state machine asks only for
  // what a process crash needs (the bytes in the page cache) and requests real
  // durability explicitly through SyncWal() (D-0014).
  o.sync_policy = options.sync_on_write ? lt::SyncPolicy::kEveryWrite : lt::SyncPolicy::kNever;
  return o;
}

}  // namespace

// lsmtree publishes a write group's sequence numbers BEFORE it inserts the
// group's records into the memtable (src/db.cpp: `last_sequence_ += Count()`
// under the lock, then the inserts after `lock.unlock()`). A reader that creates
// an iterator, or reads a key, inside that window gets a read sequence that
// already covers records which are not inserted yet. It sees a PREFIX of an
// atomic batch -- `pair-a=0, pair-b=1` -- or, for a key written twice in one
// batch, an intermediate value that never existed. LevelDB avoids this by
// publishing the sequence after the insert.
//
// Until lsmtree does, the adapter makes a write and a reader's *view creation*
// mutually exclusive (DECISIONS.md, "lsmtree read atomicity"). Flip this to
// false to see the failure: test/engine_conformance_test.cc
// AReaderNeverSeesHalfOfABatch fails within a second on lsmtree without it.
constexpr bool kGuardReadAtomicity = true;

struct LsmTreeEngine::Impl {
  Options options;

  // Exclusive: Write (so no reader can build a view mid-batch) and
  // DestroyContents (which closes and reopens the database). Shared: Get and the
  // CREATION of an iterator. Never held while iterating, so a long scan does not
  // stall writers -- that is the property this engine is here for.
  mutable std::shared_mutex mu;
  std::unique_ptr<lt::DB> db;

  // Live iterators. DestroyContents must not tear the database down underneath
  // one (it holds pointers into the DB's statistics), so it waits for them.
  // Callbacks run by Scan/ScanAll therefore must not call back into the engine:
  // if DestroyContents is waiting on this scan, a nested call would wait on it.
  std::mutex pin_mu;
  std::condition_variable pin_cv;
  int pins = 0;

  Status OpenDb() {
    std::unique_ptr<lt::DB> opened;
    const lt::Status s = lt::DB::Open(ToLsmTreeOptions(options), options.dir.string(), &opened);
    if (!s.ok()) {
      return Convert(s);
    }
    db = std::move(opened);
    return Status::Ok();
  }

  // Keeps the database alive without holding `mu`, for operations that can run
  // for a long time (a flush) and must not stall the writer queued behind them.
  // Taking it needs `mu` shared only for the instant it takes to count the pin.
  class Pin {
   public:
    explicit Pin(Impl* owner) : impl_(owner) {
      const std::shared_lock<std::shared_mutex> lock(impl_->mu);
      const std::lock_guard<std::mutex> pin_lock(impl_->pin_mu);
      ++impl_->pins;
    }
    Pin(const Pin&) = delete;
    Pin& operator=(const Pin&) = delete;
    ~Pin() {
      {
        const std::lock_guard<std::mutex> lock(impl_->pin_mu);
        --impl_->pins;
      }
      impl_->pin_cv.notify_all();
    }

   private:
    Impl* impl_;
  };

  // An iterator plus the pin that keeps the database alive for it. Declared in
  // this order so the iterator is destroyed before the pin is released.
  struct PinnedIterator {
    Impl* impl;
    std::unique_ptr<lt::Iterator> it;
    PinnedIterator(Impl* owner, std::unique_ptr<lt::Iterator> iterator)
        : impl(owner), it(std::move(iterator)) {}
    PinnedIterator(const PinnedIterator&) = delete;
    PinnedIterator& operator=(const PinnedIterator&) = delete;
    ~PinnedIterator() {
      it.reset();
      {
        const std::lock_guard<std::mutex> lock(impl->pin_mu);
        --impl->pins;
      }
      impl->pin_cv.notify_all();
    }
  };

  std::unique_ptr<PinnedIterator> NewPinnedIterator() {
    const std::shared_lock<std::shared_mutex> lock(mu);
    {
      const std::lock_guard<std::mutex> pin_lock(pin_mu);
      ++pins;
    }
    return std::make_unique<PinnedIterator>(this, db->NewIterator(lt::ReadOptions()));
  }
};

LsmTreeEngine::LsmTreeEngine(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
LsmTreeEngine::~LsmTreeEngine() = default;

Result<std::unique_ptr<Engine>> LsmTreeEngine::Open(const Options& options) {
  std::error_code ec;
  std::filesystem::create_directories(options.dir, ec);
  if (ec) {
    return Status::IoError("create " + options.dir.string() + ": " + ec.message());
  }
  auto impl = std::make_unique<Impl>();
  impl->options = options;
  RAFTKV_RETURN_IF_ERROR(impl->OpenDb());
  std::unique_ptr<Engine> engine(new LsmTreeEngine(std::move(impl)));
  return engine;
}

Result<std::optional<std::string>> LsmTreeEngine::Get(std::string_view key) const {
  const std::shared_lock<std::shared_mutex> lock(impl_->mu);
  std::string value;
  const lt::Status s = impl_->db->Get(lt::ReadOptions(), key, &value);
  if (s.ok()) {
    return std::optional<std::string>{std::move(value)};
  }
  if (s.IsNotFound()) {
    return std::optional<std::string>{};
  }
  return Convert(s);
}

Status LsmTreeEngine::Write(const Batch& batch) {
  if (batch.Empty()) {
    return Status::Ok();
  }
  lt::WriteBatch native;
  for (const auto& op : batch.Ops()) {
    if (op.is_put) {
      native.Put(op.key, op.value);
    } else {
      native.Delete(op.key);
    }
  }
  if (kGuardReadAtomicity) {
    const std::unique_lock<std::shared_mutex> lock(impl_->mu);
    return Convert(impl_->db->Write(lt::WriteOptions(), native));
  }
  const std::shared_lock<std::shared_mutex> lock(impl_->mu);
  return Convert(impl_->db->Write(lt::WriteOptions(), native));
}

Status LsmTreeEngine::Scan(std::string_view lo, std::string_view hi, size_t limit,
                           const ScanFn& fn) const {
  auto pinned = impl_->NewPinnedIterator();
  lt::Iterator& it = *pinned->it;
  if (lo.empty()) {
    it.SeekToFirst();
  } else {
    it.Seek(lo);
  }

  size_t emitted = 0;
  for (; it.Valid(); it.Next()) {
    if (!hi.empty() && it.key() >= hi) {
      break;
    }
    RAFTKV_RETURN_IF_ERROR(fn(it.key(), it.value()));
    if (limit != 0 && ++emitted >= limit) {
      break;
    }
  }
  return Convert(it.status());
}

Status LsmTreeEngine::ScanAll(const ScanFn& fn) {
  auto pinned = impl_->NewPinnedIterator();
  lt::Iterator& it = *pinned->it;
  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    RAFTKV_RETURN_IF_ERROR(fn(it.key(), it.value()));
  }
  return Convert(it.status());
}

Status LsmTreeEngine::SyncWal() {
  // Pinned, not locked: a flush takes tens of milliseconds, and holding `mu`
  // across it would make every writer (and, behind a waiting writer, every
  // reader) wait for it.
  const Impl::Pin pin(impl_.get());
  return Convert(impl_->db->FlushMemTable());
}

Status LsmTreeEngine::DestroyContents() {
  const std::unique_lock<std::shared_mutex> lock(impl_->mu);
  {
    // No new iterator can start (we hold `mu`); wait for the ones in flight.
    std::unique_lock<std::mutex> pin_lock(impl_->pin_mu);
    impl_->pin_cv.wait(pin_lock, [this] { return impl_->pins == 0; });
  }
  // Close first: the directory cannot be removed under an open database.
  impl_->db.reset();
  std::error_code ec;
  std::filesystem::remove_all(impl_->options.dir, ec);
  if (ec) {
    return Status::IoError("removing " + impl_->options.dir.string() + ": " + ec.message());
  }
  std::filesystem::create_directories(impl_->options.dir, ec);
  if (ec) {
    return Status::IoError("recreating " + impl_->options.dir.string() + ": " + ec.message());
  }
  return impl_->OpenDb();
}

std::string LsmTreeEngine::DescribeStats() const {
  const Impl::Pin pin(impl_.get());
  const lt::DbStats st = impl_->db->GetStats();
  char buf[256];
  std::snprintf(buf, sizeof(buf),
                "writes=%llu flushes=%llu compactions=%llu write_amp=%.2fx stalls=%llu",
                static_cast<unsigned long long>(st.writes),
                static_cast<unsigned long long>(st.memtable_flushes),
                static_cast<unsigned long long>(st.compactions), st.WriteAmplification(),
                static_cast<unsigned long long>(st.write_stalls));
  return buf;
}

}  // namespace raftkv::engine
