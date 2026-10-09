#pragma once

// Thin gRPC service wrapping a RaftDriver.
//
// Deliberately thin: every decision about persistence ordering, reply
// extraction, and dispatch lives in RaftDriver so it can be exercised without
// a network (see test/raft_driver_test.cc, which calls the driver's handlers
// directly). This class only translates gRPC's call shape into that API.

#include <memory>

#include <grpcpp/grpcpp.h>

#include "raft.grpc.pb.h"

#include "server/raft_driver.h"

namespace raftkv::transport {

class RaftGrpcService final : public proto::Raft::Service {
 public:
  // `driver` may be null at construction and attached later via SetDriver().
  // This exists for test setups that bind an ephemeral port -- and so must
  // start the gRPC server, with its service already registered, before the
  // driver's peer addresses (which depend on the ports every peer bound) are
  // known. A real deployment uses fixed configured ports and can construct
  // both together; nothing here is a production concern.
  explicit RaftGrpcService(std::shared_ptr<server::RaftDriver> driver = nullptr);

  void SetDriver(std::shared_ptr<server::RaftDriver> driver);

  grpc::Status RequestVote(grpc::ServerContext* ctx, const proto::Message* request,
                           proto::Message* response) override;
  grpc::Status AppendEntries(grpc::ServerContext* ctx, const proto::Message* request,
                             proto::Message* response) override;
  grpc::Status InstallSnapshot(grpc::ServerContext* ctx, const proto::Message* request,
                               proto::Message* response) override;

 private:
  std::shared_ptr<server::RaftDriver> driver_;
};

}  // namespace raftkv::transport
