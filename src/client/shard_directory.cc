#include "client/shard_directory.h"

namespace raftkv::client {
namespace {
constexpr const char* kMapKey = "shardmap";
}  // namespace

ShardDirectory::ShardDirectory(ClientOptions meta_cluster) : client_(std::move(meta_cluster)) {}

Result<std::optional<shard::ShardMap>> ShardDirectory::Load() {
  auto got = client_.Get(kMapKey);
  if (!got.IsOk()) {
    return got.GetStatus();
  }
  const std::optional<std::string>& bytes = *got;
  if (!bytes.has_value()) {
    return std::optional<shard::ShardMap>{};
  }
  auto map = shard::ShardMap::Deserialize(bytes.value());
  if (!map.IsOk()) {
    return map.GetStatus();
  }
  return std::optional<shard::ShardMap>{map.TakeValue()};
}

Result<bool> ShardDirectory::CompareAndPublish(const std::optional<shard::ShardMap>& current,
                                               const shard::ShardMap& next) {
  std::optional<std::string> expected;
  if (current.has_value()) {
    expected = current->Serialize();
  }
  auto outcome = client_.Cas(kMapKey, expected, next.Serialize());
  if (!outcome.IsOk()) {
    return outcome.GetStatus();
  }
  return outcome->applied;
}

Result<std::optional<std::string>> ShardDirectory::GetRecord(const std::string& name) {
  return client_.Get("rec/" + name);
}

Status ShardDirectory::PutRecord(const std::string& name, const std::string& value) {
  return client_.Put("rec/" + name, value);
}

Status ShardDirectory::DeleteRecord(const std::string& name) {
  return client_.Delete("rec/" + name);
}

}  // namespace raftkv::client
