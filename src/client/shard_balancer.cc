#include "client/shard_balancer.h"

#include <algorithm>

namespace raftkv::client {

ShardBalancer::ShardBalancer(ShardedClient* data, ShardDirectory* directory, ClientOptions admin,
                             BalancerOptions options)
    : data_(data), directory_(directory), admin_(std::move(admin)), options_(options) {}

Result<std::optional<SplitPlan>> ShardBalancer::Plan() {
  auto loaded = directory_->Load();
  if (!loaded.IsOk()) {
    return loaded.GetStatus();
  }
  if (!loaded->has_value()) {
    return std::optional<SplitPlan>{};
  }
  const shard::ShardMap& map = **loaded;

  for (size_t i = 0; i < map.Ranges().size(); ++i) {
    const shard::ShardRange& range = map.Ranges()[i];
    const std::string end = map.EndOf(i);

    std::vector<std::string> keys;
    std::string cursor = range.start;
    while (keys.size() <= options_.max_keys_per_shard) {
      auto page = data_->Scan(cursor, end, options_.scan_page);
      if (!page.IsOk()) {
        return page.GetStatus();
      }
      for (const auto& pair : page->pairs) {
        keys.push_back(pair.first);
      }
      if (!page->more || page->pairs.empty()) {
        break;
      }
      cursor = page->pairs.back().first;
      cursor.push_back('\0');  // the smallest key after the last one seen
    }
    if (keys.size() <= options_.max_keys_per_shard) {
      continue;
    }

    // The median of what was counted. It is strictly above the first key, and
    // so above the range's start, and below the range's end, so neither half
    // of the split comes out empty.
    SplitPlan plan;
    plan.source_shard = range.shard_id;
    plan.split_key = keys[keys.size() / 2];
    plan.keys_counted = keys.size();
    return std::optional<SplitPlan>(std::move(plan));
  }
  return std::optional<SplitPlan>{};
}

Result<bool> ShardBalancer::RunOnce(const Provision& provision) {
  auto plan = Plan();
  if (!plan.IsOk()) {
    return plan.GetStatus();
  }
  if (!plan->has_value()) {
    return false;
  }

  auto addresses = provision();
  if (!addresses.IsOk()) {
    return addresses.GetStatus();
  }

  auto loaded = directory_->Load();
  if (!loaded.IsOk()) {
    return loaded.GetStatus();
  }
  if (!loaded->has_value()) {
    return Status::Unavailable("the shard map disappeared while planning a split");
  }
  uint32_t next_id = 0;
  for (const auto& r : (*loaded)->Ranges()) {
    next_id = std::max(next_id, r.shard_id);
  }
  ++next_id;

  SplitRequest request;
  // The epoch in the id keeps a later split of the same shard from being
  // mistaken for the progress record of this one.
  request.id = "auto-" + std::to_string((*loaded)->Epoch()) + "-" +
               std::to_string((*plan)->source_shard);
  request.source_shard = (*plan)->source_shard;
  request.split_key = (*plan)->split_key;
  request.target = {next_id, (*plan)->split_key, *addresses};

  ShardSplitter splitter(directory_, admin_);
  const Status s = splitter.Split(request);
  if (!s.IsOk()) {
    return s;
  }
  return true;
}

}  // namespace raftkv::client
