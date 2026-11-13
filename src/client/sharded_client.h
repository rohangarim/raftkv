#pragma once

// A client for a key space split across several Raft groups.
//
// Point operations go to the one shard that owns the key. A scan walks the
// shards its range overlaps, in key order, so results come back sorted across
// shard boundaries without any merging.
//
// A single operation, or a Batch whose keys all live on one shard, is atomic.
// Writes spanning shards go through a two-phase-commit transaction (see
// Transaction below), so they are atomic too.
//
// Thread safety: operations may run concurrently. The mutex here guards only the
// shard map and the per-shard client cache; it is never held across a network
// call, because settling another transaction's lock can mean waiting for it.

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client/kv_client.h"
#include "client/shard_directory.h"
#include "shard/shard_map.h"

namespace raftkv::client {

class Transaction;

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

  // Applies every write if every check holds, otherwise none. Atomic across
  // shards: keys on one shard are one Raft entry, keys on several are committed
  // with two-phase commit.
  Result<BatchOutcome> Batch(const std::vector<BatchCheck>& checks,
                             const std::vector<BatchWrite>& writes);

  // Up to `limit` entries of [start, end) in key order across shards. `limit`
  // 0 means everything. `more` reports whether the range holds entries past
  // the ones returned.
  Result<ScanPage> Scan(std::string_view start, std::string_view end, size_t limit = 0);

  // A multi-key, multi-shard transaction: reads are validated and writes are
  // applied atomically at Commit. See Transaction.
  Transaction Begin();

  // Replaces the layout, e.g. after a split. Ignored unless the epoch is newer.
  void UpdateMap(shard::ShardMap map);

  // With a directory attached, a "wrong shard" answer makes the client fetch the
  // current map and route again, so a split that happens mid-operation is
  // invisible to the caller (beyond a short pause while a moving range is
  // fenced). The directory must outlive the client.
  void SetDirectory(ShardDirectory* directory);

  // How long a reader or writer waits on another transaction's lock before it
  // settles it by rolling that transaction back. A live transaction finishes
  // well inside this; only an abandoned one reaches it.
  void SetLockWait(std::chrono::milliseconds wait) { lock_wait_ = wait; }

  // Test hook: called at named points of a commit with the step's name
  // ("prewritten", "primary-committed"). Returning true stops there, as if the
  // coordinator had died.
  std::function<bool(const std::string& step)> txn_stop_after;

  uint64_t MapEpoch() const;
  uint32_t ShardFor(std::string_view key) const;

 private:
  friend class Transaction;

  struct Route {
    shard::ShardRange range;
    std::string end;  // exclusive upper bound of the range; empty if unbounded
    std::shared_ptr<KvClient> client;
  };

  Route RouteFor(std::string_view key);
  std::shared_ptr<KvClient> ClientFor(const shard::ShardRange& range);
  shard::ShardMap Snapshot() const;
  // Fetches the directory's map; true if it was newer than ours.
  bool Refresh();
  // Waits out a Moving/WrongShard answer. Returns false once the deadline passes.
  bool BackOff(const Status& why, std::chrono::steady_clock::time_point deadline);

  // Runs `op` against the owner of `key`, refreshing and settling locks and
  // retrying as needed.
  template <typename Op>
  auto Routed(std::string_view key, Op&& op);

  // Brings another transaction's lock to a conclusion: rolls it forward if that
  // transaction committed, back if it aborted, and after lock_wait_ rolls back
  // one that shows no sign of finishing.
  Status ResolveLock(const proto::LockRecord& lock);

  // Two-phase commit of `keys`. Returns true if committed, false if a validated
  // read no longer held (the caller should retry its transaction).
  struct TxnWrite {
    std::string key;
    std::optional<std::string> value;  // nullopt: delete; only meaningful if `write`
    bool write = false;
    bool validate = false;
    std::optional<std::string> expected;  // validation target; nullopt means absent
  };
  struct CommitResult {
    bool committed = false;
    std::optional<size_t> failed_index;  // into the keys passed to CommitKeys
  };
  Result<CommitResult> CommitKeys(std::vector<TxnWrite> keys);

  ClientOptions base_;
  std::chrono::milliseconds lock_wait_{300};
  std::atomic<uint64_t> txn_counter_{0};

  mutable std::mutex mutex_;
  shard::ShardMap map_;
  ShardDirectory* directory_ = nullptr;
  std::map<uint32_t, std::shared_ptr<KvClient>> clients_;
  std::map<uint32_t, std::vector<std::string>> client_addresses_;
};

// Buffers a transaction's reads and writes and commits them atomically.
//
// Get() records what it saw. Commit() locks every key involved on every shard,
// checks that each key read still holds the value it saw, and only then applies
// the writes -- so the transaction behaves as if it ran alone. If any read has
// changed, Commit() returns false and nothing was written; the caller re-runs
// the transaction. Nothing is locked or written before Commit().
class Transaction {
 public:
  Result<std::optional<std::string>> Get(const std::string& key);
  void Put(const std::string& key, std::string value);
  void Delete(const std::string& key);

  // true: committed. false: a key it read changed first; nothing was applied.
  Result<bool> Commit();

 private:
  friend class ShardedClient;
  explicit Transaction(ShardedClient* client) : client_(client) {}

  ShardedClient* client_;
  // key -> value seen (nullopt: absent). A key written after being read keeps
  // its original read so the commit still validates it.
  std::map<std::string, std::optional<std::string>> reads_;
  std::map<std::string, std::optional<std::string>> writes_;  // nullopt: delete
};

}  // namespace raftkv::client
