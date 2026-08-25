#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "admin.grpc.pb.h"

namespace raftkv::transport {

// Liveness endpoint. Holds no mutable state beyond its construction-time
// fields, so it is safe to serve from all of gRPC's handler threads at once
// without synchronization -- which is the point: a health probe must never
// contend with the replication path.
class AdminService final : public proto::Admin::Service {
 public:
  explicit AdminService(std::string node_id);

  grpc::Status Ping(grpc::ServerContext* ctx, const proto::PingRequest* req,
                    proto::PingResponse* resp) override;

 private:
  const std::string node_id_;
  const std::chrono::steady_clock::time_point started_;
};

// RAII wrapper around a gRPC server hosting this node's services.
//
// Binding with port 0 asks the OS for a free port and BoundPort() reports what
// it got. Tests need that: hardcoding ports makes the suite fail under
// parallel ctest for reasons that look like consensus bugs.
class NodeServer {
 public:
  // `address` is a gRPC bind string, e.g. "0.0.0.0:0" or "127.0.0.1:7001".
  // Returns nullptr if the address could not be bound.
  static std::unique_ptr<NodeServer> Start(const std::string& address, std::string node_id);

  ~NodeServer();

  NodeServer(const NodeServer&) = delete;
  NodeServer& operator=(const NodeServer&) = delete;

  int BoundPort() const { return bound_port_; }
  // Loopback address a client can actually dial, e.g. "127.0.0.1:53412".
  std::string ClientAddress() const;
  const std::string& NodeId() const { return node_id_; }

  // Idempotent. Called by the destructor.
  void Shutdown();

 private:
  NodeServer(std::string node_id, int bound_port, std::unique_ptr<AdminService> admin,
             std::unique_ptr<grpc::Server> server);

  const std::string node_id_;
  const int bound_port_;
  std::unique_ptr<AdminService> admin_;
  std::unique_ptr<grpc::Server> server_;
};

// Dials `address` over an insecure channel. Phase 0 has no TLS and no auth;
// see DECISIONS.md.
std::unique_ptr<proto::Admin::Stub> MakeAdminStub(const std::string& address);

}  // namespace raftkv::transport
