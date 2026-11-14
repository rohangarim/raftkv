#pragma once

#include <memory>

#include "engine/engine.h"
#include "minilsm/db.h"

namespace raftkv::engine {

// The built-in engine (src/minilsm) behind the Engine interface. Every method is
// a direct call; the only translation is Batch -> lsm::WriteBatch.
class BuiltinEngine final : public Engine {
 public:
  static Result<std::unique_ptr<Engine>> Open(const Options& options);

  std::string_view Name() const override { return "builtin"; }
  Result<std::optional<std::string>> Get(std::string_view key) const override;
  Status Write(const Batch& batch) override;
  Status Scan(std::string_view lo, std::string_view hi, size_t limit,
              const ScanFn& fn) const override;
  // Flushes and compacts down to one table first, so it blocks writers for the
  // length of a full compaction (D-0024).
  Status ScanAll(const ScanFn& fn) override;
  Status SyncWal() override;
  Status DestroyContents() override;

 private:
  explicit BuiltinEngine(std::unique_ptr<lsm::DB> db) : db_(std::move(db)) {}
  std::unique_ptr<lsm::DB> db_;
};

}  // namespace raftkv::engine
