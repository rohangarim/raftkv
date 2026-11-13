#pragma once

// A client for a key space split across several Raft groups.
//
// Point operations go to the one shard that owns the key. A scan walks the
// shards its range overlaps, in key order, so results come back sorted across
// shard boundaries without any merging.
//
// Atomicity stops at a shard boundary: each operation is linearizable on its
// own, but nothing here makes writes to two shards atomic together.

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client/kv_client.h"
#include "shard/shard_map.h"

namespace raftkv::client {

class ShardedClient {
 public:
  // `base` supplies timeouts and the client id prefix; its addresses are
  // ignored in favour of each shard's own. Each shard gets its own KvClient
  // with a distinct client id, so their sequence numbers never collide.
  ShardedClient(shard::ShardMap map, ClientOptions base);

  Result<std::optional<std::string>> Get(std::string_view key);
  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  Result<CasOutcome> Cas(std::string_view key, std::optional<std::string> expected,
                         std::string_view new_value);

  // Up to `limit` entries of [start, end) in key order across shards. `limit`
  // 0 means everything. `more` reports whether the range holds entries past
  // the ones returned.
  Result<ScanPage> Scan(std::string_view start, std::string_view end, size_t limit = 0);

  // Replaces the layout, e.g. after a split. Ignored unless the epoch is newer.
  void UpdateMap(shard::ShardMap map);

  uint64_t MapEpoch() const;
  uint32_t ShardFor(std::string_view key) const;

 private:
  KvClient* ClientFor(const shard::ShardRange& range);

  ClientOptions base_;
  mutable std::mutex mutex_;
  shard::ShardMap map_;
  std::map<uint32_t, std::unique_ptr<KvClient>> clients_;
};

}  // namespace raftkv::client
