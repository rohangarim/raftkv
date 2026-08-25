#include "transport/admin_service.h"

#include <utility>

#include <grpcpp/server_builder.h>

#include "common/log.h"

namespace raftkv::transport {

AdminService::AdminService(std::string node_id)
    : node_id_(std::move(node_id)), started_(std::chrono::steady_clock::now()) {}

grpc::Status AdminService::Ping(grpc::ServerContext* /*ctx*/, const proto::PingRequest* req,
                                proto::PingResponse* resp) {
  const auto uptime = std::chrono::steady_clock::now() - started_;
  resp->set_nonce(req->nonce());
  resp->set_node_id(node_id_);
  resp->set_uptime_ns(
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(uptime).count()));
  return grpc::Status::OK;
}

NodeServer::NodeServer(std::string node_id, int bound_port, std::unique_ptr<AdminService> admin,
                       std::unique_ptr<grpc::Server> server)
    : node_id_(std::move(node_id)),
      bound_port_(bound_port),
      admin_(std::move(admin)),
      server_(std::move(server)) {}

std::unique_ptr<NodeServer> NodeServer::Start(const std::string& address, std::string node_id) {
  auto admin = std::make_unique<AdminService>(node_id);

  int bound_port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials(), &bound_port);
  builder.RegisterService(admin.get());

  auto server = builder.BuildAndStart();
  if (server == nullptr || bound_port == 0) {
    LOG_ERROR("failed to bind %s", address.c_str());
    return nullptr;
  }

  LOG_INFO("node %s listening on port %d", node_id.c_str(), bound_port);
  return std::unique_ptr<NodeServer>(
      new NodeServer(std::move(node_id), bound_port, std::move(admin), std::move(server)));
}

NodeServer::~NodeServer() { Shutdown(); }

std::string NodeServer::ClientAddress() const { return "127.0.0.1:" + std::to_string(bound_port_); }

void NodeServer::Shutdown() {
  if (server_ != nullptr) {
    server_->Shutdown();
    server_->Wait();
    server_.reset();
  }
}

std::unique_ptr<proto::Admin::Stub> MakeAdminStub(const std::string& address) {
  return proto::Admin::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

}  // namespace raftkv::transport
