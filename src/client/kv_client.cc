#include "client/kv_client.h"

#include <algorithm>
#include <functional>
#include <thread>
#include <utility>

namespace raftkv::client {
namespace {

std::string RandomClientId() {
  std::random_device rd;
  std::mt19937_64 gen((static_cast<uint64_t>(rd()) << 32) ^ rd());
  char buf[40];
  std::snprintf(buf, sizeof(buf), "client-%016llx", static_cast<unsigned long long>(gen()));
  return buf;
}

}  // namespace

KvClient::KvClient(ClientOptions options) : options_(std::move(options)) {
  if (options_.client_id.empty()) {
    options_.client_id = RandomClientId();
  }
  addresses_ = options_.addresses;
  std::random_device rd;
  rng_.seed(options_.jitter_seed != 0 ? options_.jitter_seed
                                      : (static_cast<uint64_t>(rd()) << 32) ^ rd());
}

uint64_t KvClient::LastSeq() const {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  return next_seq_ - 1;
}

std::string KvClient::CachedLeader() const {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  return leader_;
}

uint64_t KvClient::RetryCount() const {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  return retries_;
}

std::string KvClient::NextAddress() {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  if (!leader_.empty()) {
    return leader_;
  }
  if (addresses_.empty()) {
    return std::string();
  }
  return addresses_[rotate_++ % addresses_.size()];
}

proto::Kv::Stub* KvClient::StubFor(const std::string& address) {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  auto it = stubs_.find(address);
  if (it == stubs_.end()) {
    it = stubs_
             .emplace(address, proto::Kv::NewStub(grpc::CreateChannel(
                                   address, grpc::InsecureChannelCredentials())))
             .first;
  }
  return it->second.get();
}

void KvClient::LearnAddress(const std::string& address) {
  const std::lock_guard<std::mutex> lock(state_mutex_);
  if (std::find(addresses_.begin(), addresses_.end(), address) == addresses_.end()) {
    addresses_.push_back(address);
  }
}

std::chrono::milliseconds KvClient::Jittered(std::chrono::milliseconds backoff) {
  // Between half and all of the nominal backoff. Without jitter, clients that
  // failed together retry together and re-collide on a recovering leader.
  const std::lock_guard<std::mutex> lock(state_mutex_);
  std::uniform_int_distribution<int64_t> dist(backoff.count() / 2, backoff.count());
  return std::chrono::milliseconds(dist(rng_));
}

Status KvClient::Execute(const Attempt& attempt, proto::KvResponse* response) {
  const auto deadline = std::chrono::steady_clock::now() + options_.total_timeout;
  auto backoff = options_.initial_backoff;
  std::string last_problem = "no attempt made";

  auto pause = [&] {
    const auto wait = Jittered(backoff);
    backoff = std::min(options_.max_backoff, backoff * 2);
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());
    if (remaining.count() > 0) {
      std::this_thread::sleep_for(std::min(wait, remaining));
    }
  };

  while (std::chrono::steady_clock::now() < deadline) {
    const std::string address = NextAddress();
    if (address.empty()) {
      return Status::InvalidArgument("client has no node addresses");
    }

    grpc::ClientContext ctx;
    const auto now = std::chrono::steady_clock::now();
    const auto budget =
        std::min(options_.attempt_timeout,
                 std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
    ctx.set_deadline(std::chrono::system_clock::now() + budget);

    proto::KvResponse reply;
    const grpc::Status transport = attempt(StubFor(address), &ctx, &reply);

    if (!transport.ok()) {
      // Unreachable, crashed, or too slow. Whether a write reached the server
      // is unknown, which is exactly why the retry reuses the same seq.
      last_problem = address + ": " + transport.error_message();
      {
        const std::lock_guard<std::mutex> lock(state_mutex_);
        ++retries_;
        if (leader_ == address) {
          leader_.clear();
        }
      }
      pause();
      continue;
    }

    switch (reply.code()) {
      case proto::KvResponse::OK:
      case proto::KvResponse::NOT_FOUND:
      case proto::KvResponse::CAS_MISMATCH: {
        const std::lock_guard<std::mutex> lock(state_mutex_);
        leader_ = address;
        *response = std::move(reply);
        return Status::Ok();
      }

      case proto::KvResponse::NOT_LEADER: {
        last_problem = address + ": not the leader";
        {
          const std::lock_guard<std::mutex> lock(state_mutex_);
          ++retries_;
          leader_.clear();
        }
        if (!reply.leader_address().empty() && reply.leader_address() != address) {
          // A concrete hint is worth following immediately: no backoff, since
          // nothing is wrong, we were just pointed at the wrong node.
          LearnAddress(reply.leader_address());
          const std::lock_guard<std::mutex> lock(state_mutex_);
          leader_ = reply.leader_address();
        } else {
          pause();
        }
        break;
      }

      case proto::KvResponse::UNAVAILABLE:
      case proto::KvResponse::TIMEOUT: {
        last_problem = address + ": " + reply.message();
        {
          const std::lock_guard<std::mutex> lock(state_mutex_);
          ++retries_;
          // An election is probably in progress; the leader we cached may not
          // be one any more.
          leader_.clear();
        }
        pause();
        break;
      }

      case proto::KvResponse::INVALID:
      default:
        return Status::InvalidArgument(reply.message());
    }
  }

  return Status::TimedOut(
      "gave up after " + std::to_string(options_.total_timeout.count()) +
      " ms; the operation may or may not have been applied (last: " + last_problem + ")");
}

Result<std::optional<std::string>> KvClient::Get(std::string_view key) {
  const std::lock_guard<std::mutex> op_lock(op_mutex_);
  proto::GetRequest request;
  request.set_key(std::string(key));

  proto::KvResponse response;
  const Status s = Execute([&](proto::Kv::Stub* stub, grpc::ClientContext* ctx,
                               proto::KvResponse* out) { return stub->Get(ctx, request, out); },
                           &response);
  if (!s.IsOk()) {
    return s;
  }
  if (response.code() == proto::KvResponse::NOT_FOUND) {
    return std::optional<std::string>{};
  }
  return std::optional<std::string>{response.value()};
}

Status KvClient::Put(std::string_view key, std::string_view value) {
  const std::lock_guard<std::mutex> op_lock(op_mutex_);
  proto::PutRequest request;
  request.set_client_id(options_.client_id);
  {
    const std::lock_guard<std::mutex> lock(state_mutex_);
    request.set_seq(next_seq_++);  // once per logical operation, reused by retries
  }
  request.set_key(std::string(key));
  request.set_value(std::string(value));

  proto::KvResponse response;
  return Execute([&](proto::Kv::Stub* stub, grpc::ClientContext* ctx,
                     proto::KvResponse* out) { return stub->Put(ctx, request, out); },
                 &response);
}

Status KvClient::Delete(std::string_view key) {
  const std::lock_guard<std::mutex> op_lock(op_mutex_);
  proto::DeleteRequest request;
  request.set_client_id(options_.client_id);
  {
    const std::lock_guard<std::mutex> lock(state_mutex_);
    request.set_seq(next_seq_++);
  }
  request.set_key(std::string(key));

  proto::KvResponse response;
  return Execute([&](proto::Kv::Stub* stub, grpc::ClientContext* ctx,
                     proto::KvResponse* out) { return stub->Delete(ctx, request, out); },
                 &response);
}

Result<CasOutcome> KvClient::Cas(std::string_view key, std::optional<std::string> expected,
                                 std::string_view new_value) {
  const std::lock_guard<std::mutex> op_lock(op_mutex_);
  proto::CasRequest request;
  request.set_client_id(options_.client_id);
  {
    const std::lock_guard<std::mutex> lock(state_mutex_);
    request.set_seq(next_seq_++);
  }
  request.set_key(std::string(key));
  if (expected.has_value()) {
    request.set_expected(*expected);
  }
  request.set_new_value(std::string(new_value));

  proto::KvResponse response;
  const Status s = Execute([&](proto::Kv::Stub* stub, grpc::ClientContext* ctx,
                               proto::KvResponse* out) { return stub->Cas(ctx, request, out); },
                           &response);
  if (!s.IsOk()) {
    return s;
  }
  CasOutcome outcome;
  outcome.applied = response.code() == proto::KvResponse::OK;
  if (!outcome.applied && response.has_value()) {
    outcome.current = response.value();
  }
  return outcome;
}

}  // namespace raftkv::client
