#pragma once

// Adapter onto the standalone lsmtree library (github.com/rohangarim/embedded-lsm-kv).
//
// Compiled only when -DRAFTKV_WITH_LSMTREE=ON. The library's headers are
// confined to lsmtree_engine.cc behind a pimpl: it uses the include prefix
// `lsm/`, which would otherwise be visible to (and could shadow) any raftkv
// translation unit that includes this header.

#include <memory>

#include "engine/engine.h"

namespace raftkv::engine {

class LsmTreeEngine final : public Engine {
 public:
  static Result<std::unique_ptr<Engine>> Open(const Options& options);
  ~LsmTreeEngine() override;

  std::string_view Name() const override { return "lsmtree"; }
  Result<std::optional<std::string>> Get(std::string_view key) const override;
  Status Write(const Batch& batch) override;
  Status Scan(std::string_view lo, std::string_view hi, size_t limit,
              const ScanFn& fn) const override;
  // Iterates a point-in-time view without flushing, compacting or blocking
  // writers: lsmtree iterators pin their memtables and file set at creation.
  Status ScanAll(const ScanFn& fn) override;
  // lsmtree has no explicit WAL sync, so the barrier is a memtable flush: it
  // waits out any flush in flight, rotates, and returns once the new SSTable and
  // manifest edit are fsynced. Bounded and correct, but it writes a small file
  // per call; a native SyncWal() in lsmtree would be cheaper (see D-0061).
  Status SyncWal() override;
  Status DestroyContents() override;
  std::string DescribeStats() const override;

 private:
  struct Impl;
  explicit LsmTreeEngine(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv::engine
