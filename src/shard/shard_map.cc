#include "shard/shard_map.h"

#include <algorithm>
#include <set>
#include <utility>

namespace raftkv::shard {

Result<ShardMap> ShardMap::Create(std::vector<ShardRange> ranges, uint64_t epoch) {
  if (ranges.empty()) {
    return Status::InvalidArgument("a shard map needs at least one range");
  }
  if (!ranges.front().start.empty()) {
    return Status::InvalidArgument("the first range must start at the empty key");
  }
  std::set<uint32_t> ids;
  for (size_t i = 0; i < ranges.size(); ++i) {
    if (i > 0 && !(ranges[i - 1].start < ranges[i].start)) {
      return Status::InvalidArgument("range starts must be strictly increasing");
    }
    if (!ids.insert(ranges[i].shard_id).second) {
      return Status::InvalidArgument("shard id " + std::to_string(ranges[i].shard_id) +
                                     " appears twice");
    }
    if (ranges[i].addresses.empty()) {
      return Status::InvalidArgument("shard " + std::to_string(ranges[i].shard_id) +
                                     " has no addresses");
    }
  }
  return ShardMap(std::move(ranges), epoch);
}

size_t ShardMap::IndexOf(std::string_view key) const {
  // Last range whose start is <= key. The first range starts at "", so one
  // always exists.
  auto it = std::upper_bound(
      ranges_.begin(), ranges_.end(), key,
      [](std::string_view k, const ShardRange& range) { return k < range.start; });
  return static_cast<size_t>(it - ranges_.begin()) - 1;
}

const ShardRange& ShardMap::Lookup(std::string_view key) const { return ranges_[IndexOf(key)]; }

std::string ShardMap::EndOf(size_t index) const {
  return index + 1 < ranges_.size() ? ranges_[index + 1].start : std::string();
}

std::vector<const ShardRange*> ShardMap::Overlapping(std::string_view start,
                                                     std::string_view end) const {
  std::vector<const ShardRange*> out;
  if (!end.empty() && end <= start) {
    return out;
  }
  for (size_t i = IndexOf(start); i < ranges_.size(); ++i) {
    if (!end.empty() && ranges_[i].start >= end) {
      break;
    }
    out.push_back(&ranges_[i]);
  }
  return out;
}

Result<ShardMap> ShardMap::Split(std::string_view split_key, ShardRange new_shard) const {
  if (split_key.empty()) {
    return Status::InvalidArgument("cannot split at the empty key");
  }
  const size_t index = IndexOf(split_key);
  if (ranges_[index].start == split_key) {
    return Status::InvalidArgument("split key is already a range boundary");
  }
  new_shard.start = std::string(split_key);
  std::vector<ShardRange> next = ranges_;
  next.insert(next.begin() + static_cast<std::ptrdiff_t>(index) + 1, std::move(new_shard));
  return Create(std::move(next), epoch_ + 1);
}

}  // namespace raftkv::shard
