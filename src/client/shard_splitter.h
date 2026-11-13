#pragma once

// Splits one shard's key range across two Raft groups while the cluster keeps
// serving.
//
// The moving range is unavailable for writes for a short window; everything
// else keeps working throughout. The steps, in the order that keeps every read
// linearizable:
//
//   1. fence      the source refuses writes to the moving range (reads continue)
//   2. copy       the range is copied to the target, which owns nothing yet
//   3. publish    the directory's map is swapped to the new layout (new epoch)
//   4. release    the source gives up the range, so stale clients can no longer
//                 read old data from it
//   5. activate   the target takes ownership and starts serving
//   6. purge      the source deletes the keys it no longer owns
//
// Step 4 must precede step 5. Reversed, a client holding the old map could read
// a stale value from the source after the target had already accepted a write.
//
// Progress is recorded in the directory after each step and every step is
// idempotent, so a coordinator that dies partway is resumed with Resume(): it
// repeats the step it was on and carries on.

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "client/kv_client.h"
#include "client/shard_directory.h"
#include "shard/shard_map.h"

namespace raftkv::client {

struct SplitRequest {
  // Unique name for this split; also the key of its progress record.
  std::string id;
  uint32_t source_shard = 0;
  // The upper half [split_key, ...) moves to `target`.
  std::string split_key;
  shard::ShardRange target;
};

class ShardSplitter {
 public:
  ShardSplitter(ShardDirectory* directory, ClientOptions base);

  // Starts the split, or resumes it if a record for this id already exists.
  Status Split(const SplitRequest& request);

  // Continues a split from its recorded progress.
  Status Resume(const std::string& id);

  // Test hook: called after each step is recorded with the step's name. Return
  // true to stop there, as if the coordinator had crashed.
  std::function<bool(const std::string& step)> stop_after;

 private:
  struct Record;

  Status Run(Record* record);
  Status Save(const Record& record);
  KvClient* ClientFor(const std::vector<std::string>& addresses, const std::string& tag);

  ShardDirectory* directory_;
  ClientOptions base_;
  std::map<std::string, std::unique_ptr<KvClient>> clients_;
};

}  // namespace raftkv::client
