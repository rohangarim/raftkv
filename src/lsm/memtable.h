#pragma once

// In-memory sorted table.
//
// Backed by std::map with the internal-key comparator rather than a skiplist.
// A skiplist is the classic choice and permits lock-free concurrent reads;
// std::map is chosen here because its ordering is trivially auditable and this
// engine's determinism requirement -- every replica must reach byte-identical
// state from the same command sequence -- is worth more than read concurrency
// at this stage. Concurrency is handled one level up, by a shared_mutex in DB.
//
// If memtable lookup shows up in a Phase 9 profile, the skiplist is the
// documented upgrade. That decision should come from a profile, not from
// taste.

#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "lsm/internal_key.h"

namespace raftkv::lsm {

class MemTable {
 public:
  MemTable() = default;

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Add(SequenceNumber seq, ValueType type, std::string_view key, std::string_view value);

  // Looks up `key` as of `snapshot`.
  //
  // Three distinct outcomes, and collapsing any two of them is a bug:
  //   - found a value            -> returns kValue, fills `value`
  //   - found a tombstone        -> returns kDeletion; the caller must STOP
  //                                 and not consult older tables
  //   - no record at or below the snapshot -> returns nullopt; keep looking
  std::optional<ValueType> Get(std::string_view key, SequenceNumber snapshot,
                               std::string* value) const;

  // Approximate heap footprint, used to decide when to flush.
  size_t ApproximateMemoryUsage() const { return memory_usage_; }

  bool Empty() const { return table_.empty(); }
  size_t Count() const { return table_.size(); }

  using Table = std::map<std::string, std::string, InternalKeyComparator>;
  const Table& Entries() const { return table_; }

 private:
  Table table_;
  size_t memory_usage_ = 0;
};

}  // namespace raftkv::lsm
