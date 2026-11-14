#pragma once

// The storage-engine seam.
//
// The replicated state machine needs exactly six things from whatever stores its
// data, and this header is that list and nothing more. Two engines implement it
// (see DECISIONS.md D-0059): the small built-in LSM in src/minilsm, and the
// standalone lsmtree library (github.com/rohangarim/embedded-lsm-kv) behind an
// adapter. Everything above this line -- Raft, the session table, transactions,
// range ownership, snapshots -- is written once and runs on either.
//
// The contract is stated here, in one place, because the two engines are
// different code and "same behaviour" has to mean something checkable. It is
// checked by test/engine_conformance_test.cc, which runs every case against
// every available engine, and by running the state-machine suite and the whole
// ctest run under each engine (RAFTKV_ENGINE=lsmtree ctest).

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "minilsm/status.h"

namespace raftkv::engine {

using lsm::Result;
using lsm::Status;

// An engine-neutral set of mutations, applied atomically by Engine::Write.
//
// Neither engine's own write-batch type appears in this interface, so the state
// machine can build batches without knowing which engine will receive them. The
// price is one copy of each key and value on the way in, which is small next to
// a log append and is measured in results/ rather than assumed.
class Batch {
 public:
  struct Op {
    bool is_put = true;
    std::string key;
    std::string value;  // empty for a delete
  };

  void Put(std::string_view key, std::string_view value) {
    ops_.push_back({true, std::string(key), std::string(value)});
  }
  void Delete(std::string_view key) { ops_.push_back({false, std::string(key), std::string()}); }
  void Clear() { ops_.clear(); }

  bool Empty() const { return ops_.empty(); }
  size_t Count() const { return ops_.size(); }
  const std::vector<Op>& Ops() const { return ops_; }

 private:
  std::vector<Op> ops_;
};

enum class Kind {
  kBuiltin,  // src/minilsm: simple, full compaction, consistent scan blocks writes
  kLsmTree,  // the standalone lsmtree library: leveled, pinned snapshots, block cache
};

std::string_view KindName(Kind kind);
Result<Kind> ParseKind(std::string_view name);

// True if this build contains the engine. lsmtree is optional at build time
// (-DRAFTKV_WITH_LSMTREE=OFF), so callers must ask rather than assume.
bool Available(Kind kind);
std::vector<Kind> AvailableKinds();

// The engine used when nothing says otherwise: the RAFTKV_ENGINE environment
// variable if set and valid, else the built-in one. Reading the environment here
// is what lets `RAFTKV_ENGINE=lsmtree ctest` rerun the entire suite -- every
// cluster, client, shard and transaction test -- on the other engine without
// editing any of them.
Kind DefaultKind();

struct Options {
  std::filesystem::path dir;
  Kind kind = DefaultKind();

  // Active memtable size at which it is flushed. Both engines default to 4 MiB.
  size_t memtable_bytes = size_t{4} * 1024 * 1024;

  // L0 files that trigger a compaction.
  int l0_compaction_trigger = 4;

  // Make every Write durable before it returns. Off by default and deliberately:
  // the Raft log, not the state machine, is what makes an acknowledged write
  // durable, so the apply path does not pay a second fsync (DECISIONS.md D-0014).
  // Durability of applied state is instead requested explicitly, via SyncWal(),
  // before the Raft log is allowed to discard what the state machine holds.
  bool sync_on_write = false;
};

using ScanFn = std::function<Status(std::string_view key, std::string_view value)>;

class Engine {
 public:
  virtual ~Engine() = default;

  virtual std::string_view Name() const = 0;

  // The newest value of `key`, or nullopt if absent or deleted.
  virtual Result<std::optional<std::string>> Get(std::string_view key) const = 0;

  // Applies every mutation in `batch` atomically: after a crash, all of it or
  // none of it. Within a batch, later operations on the same key win. This is
  // the property the whole state machine leans on -- an effect, its session
  // entry and the new applied index travel in one batch, so a crash can never
  // leave one without the others.
  virtual Status Write(const Batch& batch) = 0;

  // Calls `fn` for each live key in [lo, hi), in ascending byte order, stopping
  // after `limit` keys (0 means no limit). An empty `hi` is unbounded. A non-OK
  // status returned by `fn` stops the scan and is returned unchanged.
  virtual Status Scan(std::string_view lo, std::string_view hi, size_t limit,
                      const ScanFn& fn) const = 0;

  // Calls `fn` for every live key, ascending, as one consistent image of the
  // data as it was when the call began -- writes that land during the scan are
  // not visible to it. Whether concurrent writes are *blocked* during the scan
  // is engine-dependent and observable: see DECISIONS.md D-0024 and D-0061.
  // Used to build Raft snapshots.
  virtual Status ScanAll(const ScanFn& fn) = 0;

  // A durability barrier: when this returns OK, every write acknowledged before
  // the call survives a crash of the whole machine, not merely of the process.
  // The Raft layer calls it before compacting its log past what the state
  // machine has applied; skipping it there loses committed data that nothing
  // else can reconstruct.
  virtual Status SyncWal() = 0;

  // Removes all data, leaving an empty engine. Snapshot restore replaces state
  // wholesale rather than merging into it: merging would keep keys the
  // snapshot's author had deleted, and the restored replica would diverge.
  virtual Status DestroyContents() = 0;

  // Human-readable counters for benchmarks; empty if the engine has none.
  virtual std::string DescribeStats() const { return {}; }
};

Result<std::unique_ptr<Engine>> Open(const Options& options);

}  // namespace raftkv::engine
