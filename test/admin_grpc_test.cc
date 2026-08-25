// Phase 0 end-to-end toolchain proof: protoc ran, the gRPC plugin ran, a real
// server bound a real socket, and a real client got a real response back.
// If this passes, the build system is not the thing that is broken.

#include "transport/admin_service.h"

#include <memory>

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "admin.grpc.pb.h"

namespace raftkv::transport {
namespace {

class AdminGrpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    server_ = NodeServer::Start("127.0.0.1:0", "node-test");
    ASSERT_NE(server_, nullptr) << "could not bind an ephemeral port";
    stub_ = MakeAdminStub(server_->ClientAddress());
    ASSERT_NE(stub_, nullptr);
  }

  void TearDown() override {
    stub_.reset();
    server_.reset();
  }

  std::unique_ptr<NodeServer> server_;
  std::unique_ptr<proto::Admin::Stub> stub_;
};

TEST_F(AdminGrpcTest, PingRoundTripsNonceAndIdentity) {
  proto::PingRequest req;
  req.set_nonce(0xDEADBEEFCAFEULL);

  proto::PingResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  const grpc::Status status = stub_->Ping(&ctx, req, &resp);
  ASSERT_TRUE(status.ok()) << status.error_code() << ": " << status.error_message();
  EXPECT_EQ(resp.nonce(), 0xDEADBEEFCAFEULL);
  EXPECT_EQ(resp.node_id(), "node-test");
}

TEST_F(AdminGrpcTest, UptimeIsMonotonicAcrossCalls) {
  auto ping = [this](uint64_t nonce) {
    proto::PingRequest req;
    req.set_nonce(nonce);
    proto::PingResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    EXPECT_TRUE(stub_->Ping(&ctx, req, &resp).ok());
    return resp.uptime_ns();
  };

  const uint64_t first = ping(1);
  const uint64_t second = ping(2);
  EXPECT_GE(second, first);
}

TEST_F(AdminGrpcTest, DialingADeadAddressFailsCleanly) {
  // Shut the server down, then confirm the client surfaces an error rather
  // than hanging forever. Deadlines are not optional in this system.
  const std::string address = server_->ClientAddress();
  server_->Shutdown();

  auto dead_stub = MakeAdminStub(address);
  proto::PingRequest req;
  req.set_nonce(7);
  proto::PingResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  EXPECT_FALSE(dead_stub->Ping(&ctx, req, &resp).ok());
}

}  // namespace
}  // namespace raftkv::transport
