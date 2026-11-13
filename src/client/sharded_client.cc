#include "client/sharded_client.h"

#include <algorithm>
#include <random>
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

bool IsLocked(const Status& s) { return s.ErrCode() == lsm::Code::kLocked; }

void Pause() { std::this_thread::sleep_for(std::chrono::milliseconds(10)); }

std::string NewTxnId(const std::string& client_id, uint64_t counter) {
  std::random_device rd;
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%08x%08x", rd(), rd());
  return client_id + "/t" + std::to_string(counter) + "/" + buf;
}

}  // namespace

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

std::shared_ptr<KvClient> ShardedClient::ClientFor(const shard::ShardRange& range) {
  // Caller holds mutex_.
  auto it = clients_.find(range.shard_id);
  if (it != clients_.end() && client_addresses_[range.shard_id] != range.addresses) {
    clients_.erase(it);  // the group moved; dial the new addresses
    it = clients_.end();
  }
  if (it == clients_.end()) {
    ClientOptions options = base_;
    options.addresses = range.addresses;
    options.client_id = base_.client_id + "/s" + std::to_string(range.shard_id);
    it = clients_.emplace(range.shard_id, std::make_shared<KvClient>(std::move(options))).first;
    client_addresses_[range.shard_id] = range.addresses;
  }
  return it->second;
}

ShardedClient::Route ShardedClient::RouteFor(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  const shard::ShardRange& range = map_.Lookup(key);
  const size_t index = static_cast<size_t>(&range - map_.Ranges().data());
  return Route{range, map_.EndOf(index), ClientFor(range)};
}

shard::ShardMap ShardedClient::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return map_;
}

void ShardedClient::SetDirectory(ShardDirectory* directory) {
  std::lock_guard<std::mutex> lock(mutex_);
  directory_ = directory;
}

bool ShardedClient::Refresh() {
  ShardDirectory* directory = nullptr;
  uint64_t have = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    directory = directory_;
    have = map_.Epoch();
  }
  if (directory == nullptr) {
    return false;
  }
  auto loaded = directory->Load();  // a network call, made without the lock
  if (!loaded.IsOk()) {
    return false;
  }
  const std::optional<shard::ShardMap>& current = *loaded;
  if (!current.has_value() || current->Epoch() <= have) {
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (current->Epoch() > map_.Epoch()) {
    map_ = *current;
  }
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

// ---------------------------------------------------------------------------
// Settling other transactions' locks
// ---------------------------------------------------------------------------

Status ShardedClient::ResolveLock(const proto::LockRecord& lock) {
  const std::string& txid = lock.txid();
  const auto patience = std::chrono::steady_clock::now() + lock_wait_;
  while (true) {
    Route primary = RouteFor(lock.primary_key());
    auto decision = primary.client->TxnDecision(txid);
    if (!decision.IsOk()) {
      return decision.GetStatus();
    }
    const std::optional<std::string>& decided = *decision;

    if (decided.has_value() && decided.value() == "C") {
      // It committed; finish its work on the key that got in our way.
      Route blocked = RouteFor(lock.key());
      auto done = blocked.client->CommitTxn(txid, {lock.key()}, false);
      return done.IsOk() ? Status::Ok() : done.GetStatus();
    }
    if (decided.has_value() && decided.value() == "A") {
      Route blocked = RouteFor(lock.key());
      auto done = blocked.client->RollbackTxn(txid, {lock.key()});
      return done.IsOk() ? Status::Ok() : done.GetStatus();
    }

    // No decision recorded: it may simply still be running. Give it time, and
    // only then take the decision out of its hands by rolling it back. That is
    // safe either way: whichever of commit and rollback reaches the primary
    // first wins, and the loser finds out.
    if (std::chrono::steady_clock::now() >= patience) {
      auto rolled = primary.client->RollbackTxn(txid, {lock.primary_key()});
      if (!rolled.IsOk()) {
        return rolled.GetStatus();
      }
      continue;  // either aborted or turned out committed; the next read says which
    }
    Pause();
  }
}

template <typename Op>
auto ShardedClient::Routed(std::string_view key, Op&& op) {
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  while (true) {
    Route route = RouteFor(key);
    auto result = op(route.client.get());
    const Status failure = FailureOf(result);
    if (IsLocked(failure)) {
      std::optional<proto::LockRecord> lock = route.client->TakeLastLock();
      if (lock.has_value() && std::chrono::steady_clock::now() < deadline) {
        const Status settled = ResolveLock(*lock);
        if (settled.IsOk() || NeedsRouting(settled)) {
          continue;
        }
        return result;
      }
      return result;
    }
    if (!NeedsRouting(failure) || !BackOff(failure, deadline)) {
      return result;
    }
  }
}

// ---------------------------------------------------------------------------
// Single-key operations
// ---------------------------------------------------------------------------

Result<std::optional<std::string>> ShardedClient::Get(std::string_view key) {
  return Routed(key, [&](KvClient* c) { return c->Get(key); });
}

Status ShardedClient::Put(std::string_view key, std::string_view value) {
  return Routed(key, [&](KvClient* c) { return c->Put(key, value); });
}

Status ShardedClient::Delete(std::string_view key) {
  return Routed(key, [&](KvClient* c) { return c->Delete(key); });
}

Result<CasOutcome> ShardedClient::Cas(std::string_view key, std::optional<std::string> expected,
                                      std::string_view new_value) {
  return Routed(key, [&](KvClient* c) { return c->Cas(key, expected, new_value); });
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

Result<ScanPage> ShardedClient::Scan(std::string_view start, std::string_view end, size_t limit) {
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  ScanPage out;
  std::string cursor(start);
  if (!end.empty() && std::string(end) <= cursor) {
    return out;
  }

  while (true) {
    Route route = RouteFor(cursor);

    // Ask only for this shard's slice of the range.
    std::string hi = route.end;
    if (!end.empty() && (hi.empty() || std::string(end) < hi)) {
      hi = std::string(end);
    }
    const uint32_t want =
        limit == 0 ? 0
                   : static_cast<uint32_t>(std::min<size_t>(limit - out.pairs.size() + 1, 10000));
    auto page = route.client->Scan(cursor, hi, want);
    if (!page.IsOk()) {
      const Status& failure = page.GetStatus();
      if (IsLocked(failure)) {
        std::optional<proto::LockRecord> lock = route.client->TakeLastLock();
        if (lock.has_value() && std::chrono::steady_clock::now() < deadline) {
          const Status settled = ResolveLock(*lock);
          if (settled.IsOk() || NeedsRouting(settled)) {
            continue;
          }
        }
        return failure;
      }
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
    if (route.end.empty() || (!end.empty() && route.end >= std::string(end))) {
      return out;
    }
    cursor = route.end;
  }
}

// ---------------------------------------------------------------------------
// Batch: one entry per shard, or a transaction when it spans shards
// ---------------------------------------------------------------------------

Result<BatchOutcome> ShardedClient::Batch(const std::vector<BatchCheck>& checks,
                                          const std::vector<BatchWrite>& writes) {
  // One entry per distinct key, carrying both its check and its write.
  std::map<std::string, TxnWrite> by_key;
  std::map<std::string, size_t> first_check;
  for (size_t i = 0; i < checks.size(); ++i) {
    TxnWrite& t = by_key[checks[i].key];
    t.key = checks[i].key;
    if (!t.validate) {
      t.validate = true;
      t.expected = checks[i].expected;
      first_check[checks[i].key] = i;
    }
  }
  for (const BatchWrite& w : writes) {
    TxnWrite& t = by_key[w.key];
    t.key = w.key;
    t.write = true;
    t.value = w.value;  // a later write to the same key wins
  }
  std::vector<TxnWrite> keys;
  keys.reserve(by_key.size());
  for (auto& [key, t] : by_key) {
    keys.push_back(std::move(t));
  }

  auto committed = CommitKeys(keys);
  if (!committed.IsOk()) {
    return committed.GetStatus();
  }
  BatchOutcome outcome;
  const CommitResult& result = *committed;
  outcome.applied = result.committed;
  if (!outcome.applied && result.failed_index.has_value()) {
    const std::string& key = keys[result.failed_index.value()].key;
    auto it = first_check.find(key);
    if (it != first_check.end()) {
      outcome.failed_check = it->second;
    }
  }
  return outcome;
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

Transaction ShardedClient::Begin() { return Transaction(this); }

Result<std::optional<std::string>> Transaction::Get(const std::string& key) {
  auto written = writes_.find(key);
  if (written != writes_.end()) {
    return written->second;  // read your own write
  }
  auto seen = reads_.find(key);
  if (seen != reads_.end()) {
    return seen->second;  // repeatable: the same answer every time
  }
  auto got = client_->Get(key);
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  reads_[key] = *got;
  return *got;
}

void Transaction::Put(const std::string& key, std::string value) { writes_[key] = std::move(value); }

void Transaction::Delete(const std::string& key) { writes_[key] = std::nullopt; }

Result<bool> Transaction::Commit() {
  std::map<std::string, ShardedClient::TxnWrite> merged;
  for (const auto& [key, value] : reads_) {
    ShardedClient::TxnWrite& t = merged[key];
    t.key = key;
    t.validate = true;
    t.expected = value;
  }
  for (const auto& [key, value] : writes_) {
    ShardedClient::TxnWrite& t = merged[key];
    t.key = key;
    t.write = true;
    t.value = value;
  }
  std::vector<ShardedClient::TxnWrite> keys;
  keys.reserve(merged.size());
  for (auto& [key, t] : merged) {
    keys.push_back(std::move(t));
  }
  auto result = client_->CommitKeys(std::move(keys));
  if (!result.IsOk()) {
    return result.GetStatus();
  }
  return result->committed;
}

namespace {

struct Group {
  std::shared_ptr<KvClient> client;
  uint32_t shard_id = 0;
  std::vector<size_t> positions;  // indices into the caller's key vector
};

}  // namespace

Result<ShardedClient::CommitResult> ShardedClient::CommitKeys(std::vector<TxnWrite> keys) {
  if (keys.empty()) {
    return CommitResult{true, std::nullopt};
  }
  const auto deadline = std::chrono::steady_clock::now() + base_.total_timeout;
  auto stop = [&](const char* step) { return txn_stop_after && txn_stop_after(step); };

  while (true) {
    // ---- group the keys by the shard that owns them, primary's shard first ----
    std::vector<Group> groups;
    {
      std::map<uint32_t, size_t> index_of;
      for (size_t i = 0; i < keys.size(); ++i) {
        Route route = RouteFor(keys[i].key);
        auto it = index_of.find(route.range.shard_id);
        if (it == index_of.end()) {
          it = index_of.emplace(route.range.shard_id, groups.size()).first;
          groups.push_back(Group{route.client, route.range.shard_id, {}});
        }
        groups[it->second].positions.push_back(i);
      }
    }
    auto keys_of = [&](const Group& g) {
      std::vector<std::string> out;
      out.reserve(g.positions.size());
      for (size_t p : g.positions) {
        out.push_back(keys[p].key);
      }
      return out;
    };

    // ---- one shard: a single atomic batch, no locks needed ----
    if (groups.size() == 1) {
      std::vector<BatchCheck> checks;
      std::vector<BatchWrite> writes;
      std::vector<size_t> check_to_key;
      for (size_t i = 0; i < keys.size(); ++i) {
        if (keys[i].validate) {
          checks.push_back({keys[i].key, keys[i].expected});
          check_to_key.push_back(i);
        }
        if (keys[i].write) {
          writes.push_back({keys[i].key, keys[i].value});
        }
      }
      auto outcome = groups[0].client->Batch(checks, writes);
      if (outcome.IsOk()) {
        const BatchOutcome& applied = *outcome;
        CommitResult r;
        r.committed = applied.applied;
        if (!r.committed && applied.failed_check.has_value()) {
          r.failed_index = check_to_key[applied.failed_check.value()];
        }
        return r;
      }
      const Status& failure = outcome.GetStatus();
      if (IsLocked(failure)) {
        std::optional<proto::LockRecord> lock = groups[0].client->TakeLastLock();
        if (lock.has_value() && std::chrono::steady_clock::now() < deadline &&
            ResolveLock(*lock).IsOk()) {
          continue;
        }
      } else if (NeedsRouting(failure) && BackOff(failure, deadline)) {
        continue;
      }
      return failure;
    }

    // ---- several shards: two-phase commit ----
    const std::string txid = NewTxnId(base_.client_id, txn_counter_.fetch_add(1));
    const std::string primary = keys.front().key;  // keys arrive sorted
    // The primary's shard goes first: it holds the decision.
    {
      Route p = RouteFor(primary);
      std::stable_sort(groups.begin(), groups.end(), [&](const Group& a, const Group& b) {
        return a.shard_id == p.range.shard_id && b.shard_id != p.range.shard_id;
      });
    }

    std::vector<const Group*> prewritten;
    auto rollback_all = [&] {
      for (const Group* g : prewritten) {
        (void)g->client->RollbackTxn(txid, keys_of(*g));
      }
    };

    bool restart = false;
    for (const Group& g : groups) {
      std::vector<TxnKey> txkeys;
      for (size_t p : g.positions) {
        TxnKey k;
        k.key = keys[p].key;
        k.kind = keys[p].write ? (keys[p].value.has_value() ? proto::LockRecord::PUT
                                                            : proto::LockRecord::DELETE)
                               : proto::LockRecord::LOCK_ONLY;
        k.value = keys[p].value.value_or("");
        k.validate = keys[p].validate;
        k.expected_absent = keys[p].validate && !keys[p].expected.has_value();
        k.expected = keys[p].expected.value_or("");
        txkeys.push_back(std::move(k));
      }

      while (true) {
        auto reply = g.client->Prewrite(txid, primary, txkeys);
        if (!reply.IsOk()) {
          const Status& failure = reply.GetStatus();
          // Whatever happened to this group's keys is unknown; clear them too.
          (void)g.client->RollbackTxn(txid, keys_of(g));
          rollback_all();
          if (NeedsRouting(failure) && BackOff(failure, deadline)) {
            restart = true;
            break;
          }
          return failure;
        }
        const TxnReply& answer = *reply;
        if (answer.outcome == TxnReply::Outcome::kOk) {
          prewritten.push_back(&g);
          break;
        }
        if (answer.outcome == TxnReply::Outcome::kLocked) {
          if (answer.lock.has_value() && std::chrono::steady_clock::now() < deadline) {
            const Status settled = ResolveLock(answer.lock.value());
            if (settled.IsOk()) {
              continue;  // the way is clear; take the locks again
            }
          }
          (void)g.client->RollbackTxn(txid, keys_of(g));
          rollback_all();
          return Status::Unavailable("keys stayed locked by other transactions");
        }
        // kValidationFailed or kAborted: a read changed, or we were rolled back.
        CommitResult r;
        r.committed = false;
        if (answer.outcome == TxnReply::Outcome::kValidationFailed &&
            answer.failed_index.has_value()) {
          r.failed_index = g.positions[answer.failed_index.value()];
        }
        (void)g.client->RollbackTxn(txid, keys_of(g));
        rollback_all();
        return r;
      }
      if (restart) {
        break;
      }
    }
    if (restart) {
      continue;
    }
    if (stop("prewritten")) {
      return Status::Unavailable("stopped after prewritten (test hook)");
    }

    // ---- the decision: commit the primary ----
    const Group& primary_group = *prewritten.front();
    bool committed = false;
    auto decided = primary_group.client->CommitTxn(txid, keys_of(primary_group), true);
    if (decided.IsOk()) {
      committed = decided->outcome == TxnReply::Outcome::kOk;
    } else {
      // We do not know whether it landed. Ask the shard that holds the
      // decision, and if there is none yet, make one (rolling back) -- which
      // loses cleanly to a commit that is already there.
      auto decision = primary_group.client->TxnDecision(txid);
      if (!decision.IsOk()) {
        return decision.GetStatus();  // genuinely unknown; the caller is told so
      }
      const std::optional<std::string>& recorded = *decision;
      if (recorded.has_value()) {
        committed = recorded.value() == "C";
      } else {
        auto rolled = primary_group.client->RollbackTxn(txid, keys_of(primary_group));
        if (!rolled.IsOk()) {
          return rolled.GetStatus();
        }
        committed = rolled->outcome == TxnReply::Outcome::kAlreadyCommitted;
      }
    }
    if (!committed) {
      rollback_all();
      return CommitResult{false, std::nullopt};
    }
    if (stop("primary-committed")) {
      return Status::Unavailable("stopped after primary-committed (test hook)");
    }

    // ---- committed: roll the other shards forward ----
    // Failures here are tolerated. The decision is durable on the primary, so
    // anyone who meets a leftover lock will finish the job.
    for (size_t i = 1; i < prewritten.size(); ++i) {
      for (int attempt = 0; attempt < 3; ++attempt) {
        auto done = prewritten[i]->client->CommitTxn(txid, keys_of(*prewritten[i]), false);
        if (done.IsOk()) {
          break;
        }
        Pause();
      }
    }
    return CommitResult{true, std::nullopt};
  }
}

}  // namespace raftkv::client
