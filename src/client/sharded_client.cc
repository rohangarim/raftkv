#include "client/sharded_client.h"

#include <algorithm>
#include <thread>

namespace raftkv::client {

ShardedClient::ShardedClient(shard::ShardMap map, ClientOptions base)
    : base_(std::move(base)), map_(std::move(map)) {}

namespace {

Status FailureOf(const Status& s) { return s; }
template <typename T>
Status FailureOf(const Result<T>& r) {
  return r.IsOk() ? Status::Ok() : r.GetStatus();
}

bool NeedsRouting(const Status& s) {
  return s.ErrCode() == lsm::Code::kWrongShard || s.ErrCode() == lsm::Code::kMoving;
}

}  // namespace

KvClient* ShardedClient::ClientFor(const shard::ShardRange& range) {
  auto it = clients_.find(range.shard_id);
  if (it != clients_.end() && client_addresses_[range.shard_id] != range.addresses) {
    clients_.erase(it);  // the group moved; dial the new addresses
    it = clients_.end();
  }
  if (it == clients_.end()) {
    ClientOptions options = base_;
    options.addresses = range.addresses;
    options.client_id = base_.client_id + "/s" + std::to_string(range.shard_id);
    it = clients_.emplace(range.shard_id, std::make_unique<KvClient>(std::move(options))).first;
    client_addresses_[range.shard_id] = range.addresses;
  }
  return it->second.get();
}

void ShardedClient::SetDirectory(ShardDirectory* directory) {
  std::lock_guard<std::mutex> lock(mutex_);
  directory_ = directory;
}

bool ShardedClient::Refresh() {
  if (directory_ == nullptr) {
    return false;
  }
  auto loaded = directory_->Load();
  if (!loaded.IsOk()) {
    return false;
  }
  const std::optional<shard::ShardMap>& current = *loaded;
  if (!current.has_value() || current->Epoch() <= map_.Epoch()) {
    return false;
  }
  map_ = *current;
  return true;
}

bool ShardedClient::BackOff(const Status& why, std::chrono::steady_clock::time_point deadline) {
  if (std::chrono::steady_clock::now() >= deadline) {
    return false;
  }
  // A stale map is fixed by fetching a newer one. If there is none yet, or the
  // range is merely fenced, the handover is still in flight: wait for it.
  const bool refreshed = Refresh();
  if (!refreshed || why.ErrCode() == lsm::Code::kMoving) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return true;
}

template <typename Op>
auto ShardedClient::Routed(std::string_view key, Op&& op) {
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  while (true) {
    auto result = op(ClientFor(map_.Lookup(key)));
    const Status failure = FailureOf(result);
    if (!NeedsRouting(failure) || !BackOff(failure, deadline)) {
      return result;
    }
  }
}

Result<std::optional<std::string>> ShardedClient::Get(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Routed(key, [&](KvClient* c) { return c->Get(key); });
}

Status ShardedClient::Put(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Routed(key, [&](KvClient* c) { return c->Put(key, value); });
}

Status ShardedClient::Delete(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Routed(key, [&](KvClient* c) { return c->Delete(key); });
}

Result<CasOutcome> ShardedClient::Cas(std::string_view key, std::optional<std::string> expected,
                                      std::string_view new_value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Routed(key, [&](KvClient* c) { return c->Cas(key, expected, new_value); });
}

Result<BatchOutcome> ShardedClient::Batch(const std::vector<BatchCheck>& checks,
                                          const std::vector<BatchWrite>& writes) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  std::vector<bool> check_done(checks.size(), false);
  std::vector<bool> write_done(writes.size(), false);
  BatchOutcome outcome;

  struct Group {
    const shard::ShardRange* range = nullptr;
    std::vector<BatchCheck> checks;
    std::vector<size_t> check_index;
    std::vector<BatchWrite> writes;
    std::vector<size_t> write_index;
  };

  while (true) {
    // Group whatever has not been applied yet by its CURRENT owner. If the map
    // changes underneath us we come back here and regroup the remainder; items
    // already applied stay applied and are never sent again.
    std::map<uint32_t, Group> groups;
    for (size_t i = 0; i < checks.size(); ++i) {
      if (check_done[i]) continue;
      const shard::ShardRange& r = map_.Lookup(checks[i].key);
      Group& g = groups[r.shard_id];
      g.range = &r;
      g.checks.push_back(checks[i]);
      g.check_index.push_back(i);
    }
    for (size_t i = 0; i < writes.size(); ++i) {
      if (write_done[i]) continue;
      const shard::ShardRange& r = map_.Lookup(writes[i].key);
      Group& g = groups[r.shard_id];
      g.range = &r;
      g.writes.push_back(writes[i]);
      g.write_index.push_back(i);
    }
    if (groups.empty()) {
      outcome.applied = true;
      return outcome;
    }

    bool regroup = false;
    for (auto& [id, group] : groups) {
      auto result = ClientFor(*group.range)->Batch(group.checks, group.writes);
      if (!result.IsOk()) {
        const Status& failure = result.GetStatus();
        if (NeedsRouting(failure) && BackOff(failure, deadline)) {
          regroup = true;
          break;
        }
        return failure;
      }
      if (!result->applied) {
        outcome.applied = false;
        const std::optional<size_t> failed = result->failed_check;
        if (failed.has_value()) {
          outcome.failed_check = group.check_index[failed.value()];
        }
        return outcome;
      }
      for (size_t i : group.check_index) check_done[i] = true;
      for (size_t i : group.write_index) {
        write_done[i] = true;
        outcome.applied_writes.push_back(i);
      }
    }
    if (!regroup) {
      outcome.applied = true;
      return outcome;
    }
  }
}

Result<ScanPage> ShardedClient::Scan(std::string_view start, std::string_view end, size_t limit) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  ScanPage out;
  std::string cursor(start);
  if (!end.empty() && std::string(end) <= cursor) {
    return out;
  }

  while (true) {
    const shard::ShardRange& range = map_.Lookup(cursor);
    const size_t index = static_cast<size_t>(&range - map_.Ranges().data());
    const std::string shard_end = map_.EndOf(index);

    // Ask only for this shard's slice of the range.
    std::string hi = shard_end;
    if (!end.empty() && (hi.empty() || std::string(end) < hi)) {
      hi = std::string(end);
    }
    const uint32_t want =
        limit == 0 ? 0
                   : static_cast<uint32_t>(std::min<size_t>(limit - out.pairs.size() + 1, 10000));
    auto page = ClientFor(range)->Scan(cursor, hi, want);
    if (!page.IsOk()) {
      const Status& failure = page.GetStatus();
      if (NeedsRouting(failure) && BackOff(failure, deadline)) {
        continue;  // re-resolve the owner of the cursor and try again
      }
      return failure;
    }

    for (auto& pair : page->pairs) {
      if (limit != 0 && out.pairs.size() == limit) {
        out.more = true;
        return out;
      }
      out.pairs.push_back(std::move(pair));
    }
    if (page->more) {
      if (limit != 0 && out.pairs.size() == limit) {
        out.more = true;
        return out;
      }
      cursor = out.pairs.back().first;
      cursor.push_back('\0');  // resume just past the last key received
      continue;
    }
    // This shard is exhausted for the range; move to the next one.
    if (shard_end.empty() || (!end.empty() && shard_end >= std::string(end))) {
      return out;
    }
    cursor = shard_end;
  }
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
