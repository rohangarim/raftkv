#include "engine/builtin_engine.h"

#include <utility>

namespace raftkv::engine {

Result<std::unique_ptr<Engine>> BuiltinEngine::Open(const Options& options) {
  lsm::Options o;
  o.dir = options.dir;
  o.memtable_bytes = options.memtable_bytes;
  o.l0_compaction_trigger = static_cast<size_t>(options.l0_compaction_trigger);
  o.sync_on_write = options.sync_on_write;
  auto opened = lsm::DB::Open(std::move(o));
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<Engine> engine(new BuiltinEngine(opened.TakeValue()));
  return engine;
}

Result<std::optional<std::string>> BuiltinEngine::Get(std::string_view key) const {
  return db_->Get(key);
}

Status BuiltinEngine::Write(const Batch& batch) {
  if (batch.Empty()) {
    return Status::Ok();
  }
  lsm::WriteBatch native;
  for (const auto& op : batch.Ops()) {
    if (op.is_put) {
      native.Put(op.key, op.value);
    } else {
      native.Delete(op.key);
    }
  }
  return db_->Write(native);
}

Status BuiltinEngine::Scan(std::string_view lo, std::string_view hi, size_t limit,
                           const ScanFn& fn) const {
  return db_->Scan(lo, hi, limit, fn);
}

Status BuiltinEngine::ScanAll(const ScanFn& fn) { return db_->ScanAll(fn); }

Status BuiltinEngine::SyncWal() { return db_->SyncWal(); }

Status BuiltinEngine::DestroyContents() { return db_->DestroyContents(); }

}  // namespace raftkv::engine
