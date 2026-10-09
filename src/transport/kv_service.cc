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
  if (result->cas_mismatch) {
    response->set_code(proto::KvResponse::CAS_MISMATCH);
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
