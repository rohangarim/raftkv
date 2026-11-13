#pragma once

// The authoritative shard map, kept in a small Raft group of its own.
//
// Clients cache a copy and refresh it when a shard tells them their view is
// stale. Changes to the map are compare-and-swap on the serialized bytes, so two
// coordinators racing to change the layout cannot both win: the loser sees that
// the map moved on underneath it.
//
// The same group holds the split coordinator's progress records, which is what
// lets a coordinator that crashed mid-split be resumed by another.

#include <optional>
#include <string>

#include "client/kv_client.h"
#include "shard/shard_map.h"

namespace raftkv::client {

class ShardDirectory {
 public:
  explicit ShardDirectory(ClientOptions meta_cluster);

  // The stored map, or nullopt if none has been published yet.
  Result<std::optional<shard::ShardMap>> Load();

  // Replaces the stored map with `next`, but only if it is currently exactly
  // `current` (nullopt: nothing stored). Returns whether it did.
  Result<bool> CompareAndPublish(const std::optional<shard::ShardMap>& current,
                                 const shard::ShardMap& next);

  // Small named records, e.g. split progress.
  Result<std::optional<std::string>> GetRecord(const std::string& name);
  Status PutRecord(const std::string& name, const std::string& value);
  Status DeleteRecord(const std::string& name);

 private:
  KvClient client_;
};

}  // namespace raftkv::client
