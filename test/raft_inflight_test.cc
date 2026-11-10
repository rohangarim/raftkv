// A peer that accepts connections but never answers must not make the leader
// pile up a blocked thread per tick, and must not stop the rest of the cluster
// from committing.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

namespace raftkv::server {
namespace {

using testing::AsBytes;
using testing::EncodePut;
using GrpcClusterFixture = testing::GrpcClusterFixture;

// Listens and never accepts or replies: connects succeed at the kernel level,
// and every RPC then waits for its deadline.
class BlackHole {
 public:
  BlackHole() {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(fd_, 128);
    socklen_t len = sizeof(addr);
    ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    address_ = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));
  }
  ~BlackHole() { ::close(fd_); }
  const std::string& address() const { return address_; }

 private:
  int fd_ = -1;
  std::string address_;
};

class RaftInflightTest : public GrpcClusterFixture {
 protected:
  void SetUp() override {
    GrpcClusterFixture::SetUp();
    max_inflight_per_peer_ = 2;
  }
};

TEST_F(RaftInflightTest, ASilentPeerIsCappedAndTheClusterStillCommits) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  uint64_t dead = 0;
  for (auto& [id, node] : nodes_) {
    if (id != leader) {
      dead = id;
      break;
    }
  }
  CrashNode(dead);

  BlackHole hole;
  nodes_.at(leader).driver->UpdatePeerAddress(dead, hole.address());

  std::this_thread::sleep_for(std::chrono::milliseconds(800));
  EXPECT_GT(nodes_.at(leader).driver->DroppedOutbound(), 0U)
      << "messages to the silent peer were never capped";

  // The remaining follower plus the leader are still a quorum.
  const std::string cmd = EncodePut("c", 1, "k", "v");
  auto result = nodes_.at(leader).driver->ProposeAndWait(AsBytes(cmd), std::chrono::seconds(5));
  EXPECT_TRUE(result.IsOk()) << "the cluster stopped committing";
}

}  // namespace
}  // namespace raftkv::server
