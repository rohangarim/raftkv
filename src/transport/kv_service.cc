#include "transport/kv_service.h"

#include <algorithm>
#include <utility>

namespace raftkv::transport {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

}  // namespace

KvGrpcService::KvGrpcService(std::shared_ptr<server::RaftDriver> driver,
                             std::chrono::milliseconds op_timeout)
    : driver_(std::move(driver)), op_timeout_(op_timeout) {}

void KvGrpcService::SetDriver(std::shared_ptr<server::RaftDriver> driver) {
  driver_ = std::move(driver);
}

std::chrono::milliseconds KvGrpcService::TimeoutFor(const grpc::ServerContext* ctx) const {
  const auto deadline = ctx->deadline();
  if (deadline == std::chrono::system_clock::time_point::max()) {
    return op_timeout_;
  }
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::system_clock::now());
  // Leave a little headroom so the answer can still get back in time.
  return std::max(std::chrono::milliseconds(1), std::min(op_timeout_, remaining * 9 / 10));
}

void KvGrpcService::FillFromStatus(const lsm::Status& status, proto::KvResponse* response) const {
  response->set_message(status.Message());
  switch (status.ErrCode()) {
    case lsm::Code::kNotSupported:
      // The driver only returns this when it is not the leader.
      response->set_code(proto::KvResponse::NOT_LEADER);
      response->set_leader_address(driver_->AddressOf(driver_->LeaderHint()));
      break;
    case lsm::Code::kUnavailable:
      response->set_code(proto::KvResponse::UNAVAILABLE);
      break;
    case lsm::Code::kTimedOut:
      response->set_code(proto::KvResponse::TIMEOUT);
      break;
    case lsm::Code::kWrongShard:
      response->set_code(proto::KvResponse::WRONG_SHARD);
      break;
    case lsm::Code::kMoving:
      response->set_code(proto::KvResponse::MOVING);
      break;
    default:
      response->set_code(proto::KvResponse::INVALID);
      break;
  }
}

grpc::Status KvGrpcService::Mutate(grpc::ServerContext* ctx, const proto::Command& command,
                                   proto::KvResponse* response) {
  if (driver_ == nullptr) {
    response->set_code(proto::KvResponse::UNAVAILABLE);
    response->set_message("node is still starting");
    return grpc::Status::OK;
  }
  if (command.client_id().empty() || command.seq() == 0) {
    // Without an identity there is nothing to deduplicate on, so a retry could
    // apply twice. Refuse rather than silently weaken the guarantee.
    response->set_code(proto::KvResponse::INVALID);
    response->set_message("mutations require a client_id and a non-zero seq");
    return grpc::Status::OK;
  }

  const std::string bytes = command.SerializeAsString();
  auto result = driver_->ProposeAndWait(AsBytes(bytes), TimeoutFor(ctx));
  if (!result.IsOk()) {
    FillFromStatus(result.GetStatus(), response);
    return grpc::Status::OK;
  }
  if (!result->status.IsOk()) {
    response->set_code(proto::KvResponse::INVALID);
    response->set_message(result->status.ToString());
    return grpc::Status::OK;
  }
  if (result->wrong_shard) {
    response->set_code(proto::KvResponse::WRONG_SHARD);
    return grpc::Status::OK;
  }
  if (result->moving) {
    response->set_code(proto::KvResponse::MOVING);
    return grpc::Status::OK;
  }
  if (result->cas_mismatch) {
    response->set_code(proto::KvResponse::CAS_MISMATCH);
    const std::optional<uint32_t> failed = result->failed_check;
    if (failed.has_value()) {
      response->set_failed_check(failed.value());
    }
    const std::optional<std::string>& found = result->value;
    if (found.has_value()) {
      response->set_value(*found);
    }
    return grpc::Status::OK;
  }
  response->set_code(proto::KvResponse::OK);
  return grpc::Status::OK;
}

grpc::Status KvGrpcService::Get(grpc::ServerContext* ctx, const proto::GetRequest* request,
                                proto::KvResponse* response) {
  if (driver_ == nullptr) {
    response->set_code(proto::KvResponse::UNAVAILABLE);
    response->set_message("node is still starting");
    return grpc::Status::OK;
  }
  auto got = driver_->LinearizableGet(request->key(), TimeoutFor(ctx));
  if (!got.IsOk()) {
    FillFromStatus(got.GetStatus(), response);
    return grpc::Status::OK;
  }
  const std::optional<std::string>& value = *got;
  if (!value.has_value()) {
    response->set_code(proto::KvResponse::NOT_FOUND);
    return grpc::Status::OK;
  }
  response->set_code(proto::KvResponse::OK);
  response->set_value(*value);
  return grpc::Status::OK;
}

grpc::Status KvGrpcService::Scan(grpc::ServerContext* ctx, const proto::ScanRequest* request,
                                 proto::KvResponse* response) {
  if (driver_ == nullptr) {
    response->set_code(proto::KvResponse::UNAVAILABLE);
    response->set_message("node is still starting");
    return grpc::Status::OK;
  }
  constexpr uint32_t kDefaultLimit = 1000;
  constexpr uint32_t kMaxLimit = 10000;
  uint32_t limit = request->limit() == 0 ? kDefaultLimit : request->limit();
  limit = std::min(limit, kMaxLimit);

  // One past the limit, only to learn whether anything remains.
  const lsm::Status s = driver_->LinearizableScan(
      request->start(), request->end(), static_cast<size_t>(limit) + 1, TimeoutFor(ctx),
      [&](std::string_view key, std::string_view value) {
        if (static_cast<uint32_t>(response->pairs_size()) == limit) {
          response->set_more(true);
          return lsm::Status::Ok();
        }
        proto::KvPair* pair = response->add_pairs();
        pair->set_key(std::string(key));
        pair->set_value(std::string(value));
        return lsm::Status::Ok();
      });
  if (!s.IsOk()) {
    response->clear_pairs();
    response->clear_more();
    FillFromStatus(s, response);
    return grpc::Status::OK;
  }
  response->set_code(proto::KvResponse::OK);
  return grpc::Status::OK;
}

grpc::Status KvGrpcService::Batch(grpc::ServerContext* ctx, const proto::BatchRequest* request,
                                  proto::KvResponse* response) {
  constexpr int kMaxBatchOps = 10000;
  if (request->ops_size() > kMaxBatchOps || request->checks_size() > kMaxBatchOps) {
    response->set_code(proto::KvResponse::INVALID);
    response->set_message("batch is too large");
    return grpc::Status::OK;
  }
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  proto::BatchCmd* batch = command.mutable_batch();
  *batch->mutable_checks() = request->checks();
  *batch->mutable_ops() = request->ops();
  return Mutate(ctx, command, response);
}

grpc::Status KvGrpcService::Range(grpc::ServerContext* ctx, const proto::RangeRequest* request,
                                  proto::KvResponse* response) {
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  proto::RangeCmd* range = command.mutable_range();
  range->set_kind(request->kind());
  range->set_lo(request->lo());
  range->set_hi(request->hi());
  range->set_unbounded_hi(request->unbounded_hi());
  range->set_empty(request->empty());
  range->set_epoch(request->epoch());
  return Mutate(ctx, command, response);
}

grpc::Status KvGrpcService::Import(grpc::ServerContext* ctx, const proto::ImportRequest* request,
                                   proto::KvResponse* response) {
  constexpr int kMaxImport = 10000;
  if (request->puts_size() > kMaxImport) {
    response->set_code(proto::KvResponse::INVALID);
    response->set_message("import chunk is too large");
    return grpc::Status::OK;
  }
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  *command.mutable_import()->mutable_puts() = request->puts();
  return Mutate(ctx, command, response);
}

grpc::Status KvGrpcService::Put(grpc::ServerContext* ctx, const proto::PutRequest* request,
                                proto::KvResponse* response) {
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  command.mutable_put()->set_key(request->key());
  command.mutable_put()->set_value(request->value());
  return Mutate(ctx, command, response);
}

grpc::Status KvGrpcService::Delete(grpc::ServerContext* ctx, const proto::DeleteRequest* request,
                                   proto::KvResponse* response) {
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  command.mutable_delete_()->set_key(request->key());
  return Mutate(ctx, command, response);
}

grpc::Status KvGrpcService::Cas(grpc::ServerContext* ctx, const proto::CasRequest* request,
                                proto::KvResponse* response) {
  proto::Command command;
  command.set_client_id(request->client_id());
  command.set_seq(request->seq());
  auto* cas = command.mutable_cas();
  cas->set_key(request->key());
  if (request->has_expected()) {
    cas->set_expected(request->expected());
  }
  cas->set_new_value(request->new_value());
  return Mutate(ctx, command, response);
}

}  // namespace raftkv::transport
