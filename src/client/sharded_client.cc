#include "client/sharded_client.h"

#include <algorithm>

namespace raftkv::client {

ShardedClient::ShardedClient(shard::ShardMap map, ClientOptions base)
    : base_(std::move(base)), map_(std::move(map)) {}

KvClient* ShardedClient::ClientFor(const shard::ShardRange& range) {
  auto it = clients_.find(range.shard_id);
  if (it == clients_.end()) {
    ClientOptions options = base_;
    options.addresses = range.addresses;
    options.client_id = base_.client_id + "/s" + std::to_string(range.shard_id);
    it = clients_.emplace(range.shard_id, std::make_unique<KvClient>(std::move(options))).first;
  }
  return it->second.get();
}

Result<std::optional<std::string>> ShardedClient::Get(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ClientFor(map_.Lookup(key))->Get(key);
}

Status ShardedClient::Put(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ClientFor(map_.Lookup(key))->Put(key, value);
}

Status ShardedClient::Delete(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ClientFor(map_.Lookup(key))->Delete(key);
}

Result<CasOutcome> ShardedClient::Cas(std::string_view key, std::optional<std::string> expected,
                                      std::string_view new_value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ClientFor(map_.Lookup(key))->Cas(key, std::move(expected), new_value);
}

Result<BatchOutcome> ShardedClient::Batch(const std::vector<BatchCheck>& checks,
                                          const std::vector<BatchWrite>& writes) {
  std::lock_guard<std::mutex> lock(mutex_);
  struct Group {
    const shard::ShardRange* range = nullptr;
    std::vector<BatchCheck> checks;
    std::vector<size_t> check_index;
    std::vector<BatchWrite> writes;
    std::vector<size_t> write_index;
  };
  std::map<uint32_t, Group> groups;
  for (size_t i = 0; i < checks.size(); ++i) {
    const shard::ShardRange& r = map_.Lookup(checks[i].key);
    Group& g = groups[r.shard_id];
    g.range = &r;
    g.checks.push_back(checks[i]);
    g.check_index.push_back(i);
  }
  for (size_t i = 0; i < writes.size(); ++i) {
    const shard::ShardRange& r = map_.Lookup(writes[i].key);
    Group& g = groups[r.shard_id];
    g.range = &r;
    g.writes.push_back(writes[i]);
    g.write_index.push_back(i);
  }

  BatchOutcome outcome;
  for (auto& [id, group] : groups) {
    auto result = ClientFor(*group.range)->Batch(group.checks, group.writes);
    if (!result.IsOk()) {
      return result.GetStatus();
    }
    if (!result->applied) {
      outcome.applied = false;
      const std::optional<size_t> failed = result->failed_check;
      if (failed.has_value()) {
        outcome.failed_check = group.check_index[failed.value()];
      }
      return outcome;
    }
    outcome.applied_writes.insert(outcome.applied_writes.end(), group.write_index.begin(),
                                  group.write_index.end());
  }
  outcome.applied = true;
  return outcome;
}

Result<ScanPage> ShardedClient::Scan(std::string_view start, std::string_view end, size_t limit) {
  std::lock_guard<std::mutex> lock(mutex_);
  ScanPage out;
  const auto ranges = map_.Overlapping(start, end);
  for (const shard::ShardRange* range : ranges) {
    const size_t index = static_cast<size_t>(range - map_.Ranges().data());
    const std::string shard_end = map_.EndOf(index);

    // Clip the request to this shard so it never asks for keys it doesn't own.
    std::string lo = std::max(std::string(start), range->start);
    std::string hi = shard_end;
    if (!end.empty() && (hi.empty() || std::string(end) < hi)) {
      hi = std::string(end);
    }

    KvClient* client = ClientFor(*range);
    while (true) {
      const uint32_t want =
          limit == 0 ? 0 : static_cast<uint32_t>(std::min<size_t>(limit - out.pairs.size() + 1, 10000));
      auto page = client->Scan(lo, hi, want);
      if (!page.IsOk()) {
        return page.GetStatus();
      }
      for (auto& pair : page->pairs) {
        if (limit != 0 && out.pairs.size() == limit) {
          out.more = true;
          return out;
        }
        out.pairs.push_back(std::move(pair));
      }
      if (!page->more) {
        break;
      }
      if (limit != 0 && out.pairs.size() == limit) {
        out.more = true;
        return out;
      }
      lo = out.pairs.back().first;
      lo.push_back('\0');  // resume just past the last key received
    }
  }
  return out;
}

void ShardedClient::UpdateMap(shard::ShardMap map) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (map.Epoch() > map_.Epoch()) {
    map_ = std::move(map);
  }
}

uint64_t ShardedClient::MapEpoch() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return map_.Epoch();
}

uint32_t ShardedClient::ShardFor(std::string_view key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return map_.Lookup(key).shard_id;
}

}  // namespace raftkv::client
