#include "shard/shard_map.h"

#include <algorithm>
#include <set>
#include <utility>

#include "lsm/coding.h"

namespace raftkv::shard {
namespace {

// Little-endian reads by index, so the bounds are explicit at every call.
uint32_t ReadU32(std::string_view v) {
  return static_cast<uint32_t>(static_cast<uint8_t>(v[0])) |
         static_cast<uint32_t>(static_cast<uint8_t>(v[1])) << 8 |
         static_cast<uint32_t>(static_cast<uint8_t>(v[2])) << 16 |
         static_cast<uint32_t>(static_cast<uint8_t>(v[3])) << 24;
}

uint64_t ReadU64(std::string_view v) {
  return static_cast<uint64_t>(ReadU32(v)) | static_cast<uint64_t>(ReadU32(v.substr(4))) << 32;
}

}  // namespace

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

std::string ShardMap::Serialize() const {
  std::string out;
  lsm::PutFixed64(&out, epoch_);
  lsm::PutFixed32(&out, static_cast<uint32_t>(ranges_.size()));
  for (const ShardRange& r : ranges_) {
    lsm::PutFixed32(&out, r.shard_id);
    lsm::PutLengthPrefixed(&out, r.start);
    lsm::PutFixed32(&out, static_cast<uint32_t>(r.addresses.size()));
    for (const std::string& a : r.addresses) {
      lsm::PutLengthPrefixed(&out, a);
    }
  }
  return out;
}

Result<ShardMap> ShardMap::Deserialize(std::string_view bytes) {
  auto corrupt = [] { return Status::Corruption("shard map is malformed"); };
  if (bytes.size() < 12) {
    return corrupt();
  }
  const uint64_t epoch = ReadU64(bytes);
  const uint32_t count = ReadU32(bytes.substr(8));
  std::string_view rest = bytes.substr(12);
  std::vector<ShardRange> ranges;
  for (uint32_t i = 0; i < count; ++i) {
    if (rest.size() < 4) {
      return corrupt();
    }
    ShardRange r;
    r.shard_id = ReadU32(rest);
    rest.remove_prefix(4);
    std::string_view start;
    if (!lsm::GetLengthPrefixed(&rest, &start) || rest.size() < 4) {
      return corrupt();
    }
    r.start = std::string(start);
    const uint32_t naddr = ReadU32(rest);
    rest.remove_prefix(4);
    for (uint32_t a = 0; a < naddr; ++a) {
      std::string_view addr;
      if (!lsm::GetLengthPrefixed(&rest, &addr)) {
        return corrupt();
      }
      r.addresses.emplace_back(addr);
    }
    ranges.push_back(std::move(r));
  }
  if (!rest.empty()) {
    return corrupt();
  }
  return Create(std::move(ranges), epoch);
}

}  // namespace raftkv::shard
