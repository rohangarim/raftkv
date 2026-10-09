#pragma once

// Client-facing key-value service.
//
// Thin by design: it translates between the client protocol and RaftDriver and
// owns no state. Every response is a gRPC OK carrying a KvResponse code,
// because a transport failure and a protocol answer ("not the leader, try
// there") are different things and a client must be able to tell them apart.

#include <chrono>
#include <memory>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"

#include "server/raft_driver.h"

namespace raftkv::transport {

class KvGrpcService final : public proto::Kv::Service {
 public:
  // `driver` may be attached later; see RaftGrpcService for why.
  explicit KvGrpcService(std::shared_ptr<server::RaftDriver> driver = nullptr,
                         std::chrono::milliseconds op_timeout = std::chrono::milliseconds(2000));

  void SetDriver(std::shared_ptr<server::RaftDriver> driver);

  grpc::Status Get(grpc::ServerContext* ctx, const proto::GetRequest* request,
                   proto::KvResponse* response) override;
  grpc::Status Put(grpc::ServerContext* ctx, const proto::PutRequest* request,
                   proto::KvResponse* response) override;
  grpc::Status Delete(grpc::ServerContext* ctx, const proto::DeleteRequest* request,
                      proto::KvResponse* response) override;
  grpc::Status Cas(grpc::ServerContext* ctx, const proto::CasRequest* request,
                   proto::KvResponse* response) override;

 private:
  // Never wait longer than the caller will: working on a request whose client
  // has already given up is pure waste.
  std::chrono::milliseconds TimeoutFor(const grpc::ServerContext* ctx) const;

  void FillFromStatus(const lsm::Status& status, proto::KvResponse* response) const;
  grpc::Status Mutate(grpc::ServerContext* ctx, const proto::Command& command,
                      proto::KvResponse* response);

  std::shared_ptr<server::RaftDriver> driver_;
  std::chrono::milliseconds op_timeout_;
};

}  // namespace raftkv::transport
