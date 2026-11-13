#pragma once

// Which shard owns which keys.
//
// Shards are contiguous key ranges, not hash buckets. A hash ring spreads load
// evenly but scatters neighbours, so a range scan would have to ask every shard.
// With ranges, a scan touches only the shards its range overlaps, and a sorted
// layer such as SQL's primary-key order maps straight onto it.
//
// A ShardMap is immutable and carries an epoch. Changing the layout (Split)
// returns a new map with a higher epoch, so a client can tell a stale map from
// a current one by comparing numbers rather than contents.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "lsm/status.h"

namespace raftkv::shard {

using lsm::Result;
using lsm::Status;

struct ShardRange {
  uint32_t shard_id = 0;
  // Inclusive lower bound. The first range must start at "".
  std::string start;
  // Client-facing addresses of the Raft group that serves this range.
  std::vector<std::string> addresses;
};

class ShardMap {
 public:
  // `ranges` must be sorted by `start`, with unique starts, the first being "",
  // and unique shard ids. Each range ends where the next begins; the last is
  // unbounded.
  static Result<ShardMap> Create(std::vector<ShardRange> ranges, uint64_t epoch = 1);

  // The single range that owns `key`.
  const ShardRange& Lookup(std::string_view key) const;

  // Every range intersecting [start, end), in key order. An empty `end` means
  // unbounded.
  std::vector<const ShardRange*> Overlapping(std::string_view start, std::string_view end) const;

  // Exclusive upper bound of ranges()[index]; empty for the last (unbounded).
  std::string EndOf(size_t index) const;

  // A new map in which the range containing `split_key` is cut in two at that
  // key. The upper half goes to `new_shard`. `split_key` must lie strictly
  // inside an existing range, so neither half comes out empty.
  Result<ShardMap> Split(std::string_view split_key, ShardRange new_shard) const;

  const std::vector<ShardRange>& Ranges() const { return ranges_; }
  uint64_t Epoch() const { return epoch_; }

 private:
  ShardMap(std::vector<ShardRange> ranges, uint64_t epoch)
      : ranges_(std::move(ranges)), epoch_(epoch) {}

  size_t IndexOf(std::string_view key) const;

  std::vector<ShardRange> ranges_;
  uint64_t epoch_ = 1;
};

}  // namespace raftkv::shard
