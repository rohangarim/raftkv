#include "transport/raft_service.h"

#include <gtest/gtest.h>

namespace raftkv::transport {
namespace {

// A restarting node's gRPC server listens before its driver is attached (the
// peer addresses depend on every peer's port). Peers retry votes at once, so a
// handler must answer UNAVAILABLE rather than dereference the empty pointer.
// RequestVote once lacked the check the other two handlers had, and crashed
// the process (seen as SIGSEGV in the node-kill linearizability campaign).
TEST(RaftGrpcServiceTest, RpcsBeforeTheDriverIsAttachedAreUnavailable) {
  RaftGrpcService service;
  proto::Message request;
  proto::Message response;
  grpc::ServerContext ctx;

  EXPECT_EQ(service.RequestVote(&ctx, &request, &response).error_code(),
            grpc::StatusCode::UNAVAILABLE);
  EXPECT_EQ(service.AppendEntries(&ctx, &request, &response).error_code(),
            grpc::StatusCode::UNAVAILABLE);
  EXPECT_EQ(service.InstallSnapshot(&ctx, &request, &response).error_code(),
            grpc::StatusCode::UNAVAILABLE);
}

}  // namespace
}  // namespace raftkv::transport
