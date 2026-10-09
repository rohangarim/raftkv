#pragma once

// Client library for the replicated key-value store.
//
// What it does for the caller:
//   - finds the leader: caches it, follows NOT_LEADER hints, and rotates
//     through the known nodes when it has none;
//   - retries transient failures with jittered exponential backoff;
//   - makes retries safe: every mutating operation is assigned a (client_id,
//     seq) ONCE, and every retry of that operation reuses it. If an attempt
//     times out and the first attempt had in fact committed, the retry is
//     recognised by the state machine and answered from its session table
//     instead of being applied a second time.
//
// What it does not do: concurrent operations. A session table keeps only a
// client's most recent result, so two operations in flight under one client_id
// could have the later one's sequence number overtake the earlier one's retry.
// Operations are therefore serialised under one mutex; use one KvClient per
// thread that needs parallelism.
//
// Reporting an unknown outcome honestly matters to anything that checks
// histories for linearizability: kTimedOut on a write means the write MAY have
// been applied. It is not a failure, and a recorder must not log it as one.

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"
#include "lsm/status.h"

namespace raftkv::client {

using lsm::Result;
using lsm::Status;

struct ClientOptions {
  // Any subset of the cluster is enough to start; leaders are learned from
  // redirects and added here.
  std::vector<std::string> addresses;

  // Must be unique per logical client and STABLE across its own restarts if it
  // wants exactly-once across them. Generated at random when empty.
  std::string client_id;

  std::chrono::milliseconds attempt_timeout{1000};
  std::chrono::milliseconds total_timeout{10000};
  std::chrono::milliseconds initial_backoff{20};
  std::chrono::milliseconds max_backoff{400};

  // Seeds the backoff jitter so a test run is reproducible. 0 means random.
  uint64_t jitter_seed = 0;
};

struct CasOutcome {
  // True if the swap happened. False means the comparison failed: the
  // operation was applied and the answer is no.
  bool applied = false;
  // On a failed comparison, the value that was found (nullopt if absent).
  std::optional<std::string> current;
};

class KvClient {
 public:
  explicit KvClient(ClientOptions options);

  // Linearizable read.
  Result<std::optional<std::string>> Get(std::string_view key);
  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  // `expected == nullopt` means "only if the key does not exist".
  Result<CasOutcome> Cas(std::string_view key, std::optional<std::string> expected,
                         std::string_view new_value);

  const std::string& ClientId() const { return options_.client_id; }
  // Last sequence number handed out; 0 if none yet.
  uint64_t LastSeq() const;
  // Address currently believed to be the leader; empty if unknown.
  std::string CachedLeader() const;
  // Count of attempts that did not succeed, for tests and diagnostics.
  uint64_t RetryCount() const;

 private:
  // One attempt: fills `response`, returns the transport-level outcome.
  using Attempt =
      std::function<grpc::Status(proto::Kv::Stub*, grpc::ClientContext*, proto::KvResponse*)>;

  // Runs `attempt` against successive nodes until it yields a definitive
  // answer or the total budget is spent. On success `response` holds the
  // server's final word (OK, NOT_FOUND, CAS_MISMATCH).
  Status Execute(const Attempt& attempt, proto::KvResponse* response);

  std::string NextAddress();
  proto::Kv::Stub* StubFor(const std::string& address);
  void LearnAddress(const std::string& address);
  std::chrono::milliseconds Jittered(std::chrono::milliseconds backoff);

  ClientOptions options_;

  // Serialises operations; see the header comment.
  std::mutex op_mutex_;
  mutable std::mutex state_mutex_;
  std::vector<std::string> addresses_;
  std::string leader_;
  size_t rotate_ = 0;
  uint64_t next_seq_ = 1;
  uint64_t retries_ = 0;
  std::map<std::string, std::unique_ptr<proto::Kv::Stub>> stubs_;
  std::mt19937_64 rng_;
};

}  // namespace raftkv::client
