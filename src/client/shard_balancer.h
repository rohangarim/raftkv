#pragma once

// Decides when a shard has grown too large and splits it.
//
// The decision is deliberately simple: count the keys in each shard's range (up
// to a cap) and, for the first one over the limit, split at its median key. The
// split itself is ShardSplitter's, so it is the same live, resumable procedure
// an operator would run by hand; this class only supplies the "when" and the
// "where".
//
// Counting reads the shard's data through linearizable scans, so Plan() costs
// about one pass over a shard at most `max_keys_per_shard` keys long. It is meant
// to run now and then from one coordinator, not on a hot path, and it holds the
// keys of one shard in memory while it works.

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "client/kv_client.h"
#include "client/shard_directory.h"
#include "client/sharded_client.h"
#include "client/shard_splitter.h"

namespace raftkv::client {

struct BalancerOptions {
  // A shard holding more keys than this is split.
  size_t max_keys_per_shard = 100000;
  // Rows fetched per scan request while counting.
  uint32_t scan_page = 500;
};

struct SplitPlan {
  uint32_t source_shard = 0;
  // The upper half [split_key, ...) would move to a new shard.
  std::string split_key;
  // Keys counted in the shard (stops at max_keys_per_shard + 1).
  size_t keys_counted = 0;
};

class ShardBalancer {
 public:
  // `data` is a client for reading the shards; `directory` holds the layout.
  // `admin` carries the timeouts and client id for the split coordinator.
  ShardBalancer(ShardedClient* data, ShardDirectory* directory, ClientOptions admin,
                BalancerOptions options = {});

  // The split this balancer would make now, or nullopt if no shard is over the limit.
  Result<std::optional<SplitPlan>> Plan();

  // Plans and, if a split is due, performs it onto the group whose client
  // addresses `provision` returns. Returns whether a split happened.
  using Provision = std::function<Result<std::vector<std::string>>()>;
  Result<bool> RunOnce(const Provision& provision);

 private:
  ShardedClient* data_;
  ShardDirectory* directory_;
  ClientOptions admin_;
  BalancerOptions options_;
};

}  // namespace raftkv::client
