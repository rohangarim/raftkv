#include "transport/raft_service.h"

#include <atomic>
#include <memory>
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
  std::atomic_store(&driver_, std::move(driver));
}

grpc::Status RaftGrpcService::RequestVote(grpc::ServerContext* /*ctx*/,
                                          const proto::Message* request, proto::Message* response) {
  // One atomic read: the server listens before SetDriver runs on a restart, so
  // the pointer can still be null, and it must not change between check and use.
  const auto driver = std::atomic_load(&driver_);
  if (driver == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "driver not yet attached");
  }
  return ToGrpcStatus(driver->HandleRequestVote(*request, response));
}

grpc::Status RaftGrpcService::AppendEntries(grpc::ServerContext* /*ctx*/,
                                            const proto::Message* request,
                                            proto::Message* response) {
  const auto driver = std::atomic_load(&driver_);
  if (driver == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "driver not yet attached");
  }
  return ToGrpcStatus(driver->HandleAppendEntries(*request, response));
}

grpc::Status RaftGrpcService::InstallSnapshot(grpc::ServerContext* /*ctx*/,
                                              const proto::Message* request,
                                              proto::Message* response) {
  const auto driver = std::atomic_load(&driver_);
  if (driver == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "driver not yet attached");
  }
  return ToGrpcStatus(driver->HandleInstallSnapshot(*request, response));
}

}  // namespace raftkv::transport
