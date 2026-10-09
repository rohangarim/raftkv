#include "transport/raft_service.h"

#include <utility>

namespace raftkv::transport {
namespace {

grpc::Status ToGrpcStatus(const lsm::Status& s) {
  if (s.IsOk()) {
    return grpc::Status::OK;
  }
  return grpc::Status(grpc::StatusCode::INTERNAL, s.ToString());
}

}  // namespace

RaftGrpcService::RaftGrpcService(std::shared_ptr<server::RaftDriver> driver)
    : driver_(std::move(driver)) {}

void RaftGrpcService::SetDriver(std::shared_ptr<server::RaftDriver> driver) {
  driver_ = std::move(driver);
}

grpc::Status RaftGrpcService::RequestVote(grpc::ServerContext* /*ctx*/,
                                          const proto::Message* request, proto::Message* response) {
  return ToGrpcStatus(driver_->HandleRequestVote(*request, response));
}

grpc::Status RaftGrpcService::AppendEntries(grpc::ServerContext* /*ctx*/,
                                            const proto::Message* request,
                                            proto::Message* response) {
  if (driver_ == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "driver not yet attached");
  }
  return ToGrpcStatus(driver_->HandleAppendEntries(*request, response));
}

grpc::Status RaftGrpcService::InstallSnapshot(grpc::ServerContext* /*ctx*/,
                                              const proto::Message* /*request*/,
                                              proto::Message* /*response*/) {
  // RaftNode::Step does not handle MSG_SNAPSHOT yet -- that is Phase 5. This
  // returns a real gRPC error rather than silently succeeding at nothing, so
  // a caller that accidentally exercises this path fails loudly.
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                      "InstallSnapshot is not implemented until Phase 5");
}

}  // namespace raftkv::transport
