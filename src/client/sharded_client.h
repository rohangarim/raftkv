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
#include "client/shard_directory.h"
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

  // Atomic within each shard, but NOT across shards: the keys are grouped by
  // owner and each group is one Raft entry, applied in shard order. If a later
  // group's check fails, earlier groups stay applied and are listed in
  // `applied_writes` so the caller can undo them.
  Result<BatchOutcome> Batch(const std::vector<BatchCheck>& checks,
                             const std::vector<BatchWrite>& writes);

  // Up to `limit` entries of [start, end) in key order across shards. `limit`
  // 0 means everything. `more` reports whether the range holds entries past
  // the ones returned.
  Result<ScanPage> Scan(std::string_view start, std::string_view end, size_t limit = 0);

  // Replaces the layout, e.g. after a split. Ignored unless the epoch is newer.
  void UpdateMap(shard::ShardMap map);

  // With a directory attached, a "wrong shard" answer makes the client fetch the
  // current map and route again, so a split that happens mid-operation is
  // invisible to the caller (beyond a short pause while a moving range is
  // fenced). The directory must outlive the client.
  void SetDirectory(ShardDirectory* directory);

  uint64_t MapEpoch() const;
  uint32_t ShardFor(std::string_view key) const;

 private:
  KvClient* ClientFor(const shard::ShardRange& range);
  // Fetches the directory's map; true if it was newer than ours. Caller holds mutex_.
  bool Refresh();
  // Waits out a Moving/WrongShard answer. Returns false once the deadline passes.
  bool BackOff(const Status& why, std::chrono::steady_clock::time_point deadline);

  // Runs `op` against the owner of `key`, refreshing and retrying while the
  // answer is WrongShard or Moving. Caller holds mutex_.
  template <typename Op>
  auto Routed(std::string_view key, Op&& op);

  ClientOptions base_;
  mutable std::mutex mutex_;
  shard::ShardMap map_;
  ShardDirectory* directory_ = nullptr;
  std::map<uint32_t, std::unique_ptr<KvClient>> clients_;
  std::map<uint32_t, std::vector<std::string>> client_addresses_;
};

}  // namespace raftkv::client
