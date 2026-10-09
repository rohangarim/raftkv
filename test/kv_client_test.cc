// The client library against a real cluster: real gRPC, real disk, real
// elections. The properties that matter are the ones a caller relies on without
// seeing them -- that it finds the leader, that it survives losing one, and
// that a retry never applies a write twice.

#include "client/kv_client.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "grpc_cluster_fixture.h"

namespace raftkv::client {
namespace {

using server::testing::GrpcClusterFixture;

class KvClientTest : public GrpcClusterFixture {
 protected:
  std::vector<std::string> AllAddresses() const {
    std::vector<std::string> out;
    out.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
      out.push_back(node.address);
    }
    return out;
  }

  ClientOptions Options(std::vector<std::string> addresses, std::string client_id = "") const {
    ClientOptions o;
    o.addresses = std::move(addresses);
    o.client_id = std::move(client_id);
    o.jitter_seed = 42;
    return o;
  }

  uint64_t AnyFollower(uint64_t leader) const {
    for (const auto& [id, node] : nodes_) {
      if (id != leader) {
        return id;
      }
    }
    return 0;
  }
};

TEST_F(KvClientTest, PutThenGetRoundTrips) {
  StartCluster({1, 2, 3});
  ASSERT_NE(WaitForLeader(), 0U);
  KvClient client(Options(AllAddresses()));

  ASSERT_TRUE(client.Put("greeting", "hello").IsOk());
  auto got = client.Get("greeting");
  ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
  ASSERT_TRUE(got->has_value());
  EXPECT_EQ(**got, "hello");

  auto missing = client.Get("never-written");
  ASSERT_TRUE(missing.IsOk());
  EXPECT_FALSE(missing->has_value());
}

TEST_F(KvClientTest, AClientThatOnlyKnowsAFollowerFindsTheLeader) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  const uint64_t follower = AnyFollower(leader);

  KvClient client(Options({nodes_.at(follower).address}));
  ASSERT_TRUE(client.Put("k", "v").IsOk());
  EXPECT_EQ(client.CachedLeader(), nodes_.at(leader).address)
      << "the client did not follow the redirect to the leader";
  EXPECT_GE(client.RetryCount(), 1U) << "reaching the leader through a follower must take a hop";

  // Having found the leader, further operations go straight there.
  const uint64_t retries_before = client.RetryCount();
  ASSERT_TRUE(client.Put("k2", "v2").IsOk());
  EXPECT_EQ(client.RetryCount(), retries_before);
}

TEST_F(KvClientTest, DeleteAndCompareAndSwapBehaveAsSpecified) {
  StartCluster({1, 2, 3});
  ASSERT_NE(WaitForLeader(), 0U);
  KvClient client(Options(AllAddresses()));

  // Create-if-absent succeeds once.
  auto created = client.Cas("lock", std::nullopt, "owner-a");
  ASSERT_TRUE(created.IsOk()) << created.GetStatus().ToString();
  EXPECT_TRUE(created->applied);

  // A second attempt loses, is NOT an error, and reports what it found.
  auto contended = client.Cas("lock", std::nullopt, "owner-b");
  ASSERT_TRUE(contended.IsOk());
  EXPECT_FALSE(contended->applied);
  ASSERT_TRUE(contended->current.has_value());
  EXPECT_EQ(*contended->current, "owner-a");

  // Swap with the right expectation.
  auto swapped = client.Cas("lock", std::string("owner-a"), "owner-c");
  ASSERT_TRUE(swapped.IsOk());
  EXPECT_TRUE(swapped->applied);
  auto got = client.Get("lock");
  ASSERT_TRUE(got.IsOk());
  EXPECT_EQ(**got, "owner-c");

  ASSERT_TRUE(client.Delete("lock").IsOk());
  auto gone = client.Get("lock");
  ASSERT_TRUE(gone.IsOk());
  EXPECT_FALSE(gone->has_value());
}

TEST_F(KvClientTest, SequenceNumbersAdvanceOncePerOperation) {
  StartCluster({1, 2, 3});
  ASSERT_NE(WaitForLeader(), 0U);
  KvClient client(Options(AllAddresses(), "fixed-id"));
  EXPECT_EQ(client.LastSeq(), 0U);
  ASSERT_TRUE(client.Put("a", "1").IsOk());
  EXPECT_EQ(client.LastSeq(), 1U);
  ASSERT_TRUE(client.Get("a").IsOk());
  EXPECT_EQ(client.LastSeq(), 1U) << "a read must not consume a sequence number";
  ASSERT_TRUE(client.Put("b", "2").IsOk());
  EXPECT_EQ(client.LastSeq(), 2U);
}

// What a retry after an unknown outcome looks like on the wire: the same
// (client_id, seq) arrives again. Sent directly, so the test controls exactly
// what the server sees rather than hoping the client times out at the right
// moment.
TEST_F(KvClientTest, ARepeatedSequenceNumberIsAnsweredFromTheSessionNotReapplied) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  auto stub = proto::Kv::NewStub(
      grpc::CreateChannel(nodes_.at(leader).address, grpc::InsecureChannelCredentials()));

  proto::CasRequest create;
  create.set_client_id("retrier");
  create.set_seq(1);
  create.set_key("once");
  create.set_new_value("created");

  auto send = [&](const proto::CasRequest& req) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    proto::KvResponse resp;
    const grpc::Status s = stub->Cas(&ctx, req, &resp);
    EXPECT_TRUE(s.ok()) << s.error_message();
    return resp;
  };

  EXPECT_EQ(send(create).code(), proto::KvResponse::OK);
  // Re-applying a create-if-absent would find the key present and report
  // CAS_MISMATCH, so OK here proves the repeat was not applied a second time.
  EXPECT_EQ(send(create).code(), proto::KvResponse::OK)
      << "a retry of an already-applied command was applied again";

  // A brand-new sequence number IS a new operation, and loses.
  proto::CasRequest again = create;
  again.set_seq(2);
  EXPECT_EQ(send(again).code(), proto::KvResponse::CAS_MISMATCH);
}

// A process that restarts with the same client_id but a fresh sequence counter
// sends new commands under sequence numbers the cluster has already seen.
// Before the session digest this was acknowledged and silently dropped: the
// first write applied and the next nine returned OK without being applied. Now
// it fails loudly, which is the only safe thing to do with a write the system
// cannot honestly claim to have performed.
TEST_F(KvClientTest, ARestartedClientThatReusesItsIdFailsLoudlyInsteadOfLosingWrites) {
  StartCluster({1, 2, 3});
  ASSERT_NE(WaitForLeader(), 0U);

  {
    KvClient first_life(Options(AllAddresses(), "reused-id"));
    ASSERT_TRUE(first_life.Put("original", "kept").IsOk());
  }

  KvClient second_life(Options(AllAddresses(), "reused-id"));  // counter back at 1
  const Status s = second_life.Put("different-key", "would-be-lost");
  EXPECT_FALSE(s.IsOk()) << "a write the cluster never applied was acknowledged";
  EXPECT_EQ(s.ErrCode(), lsm::Code::kInvalidArgument) << s.ToString();

  KvClient observer(Options(AllAddresses()));
  auto kept = observer.Get("original");
  ASSERT_TRUE(kept.IsOk());
  EXPECT_EQ(**kept, "kept") << "the original write must be untouched";
  auto absent = observer.Get("different-key");
  ASSERT_TRUE(absent.IsOk());
  EXPECT_FALSE(absent->has_value());
}

TEST_F(KvClientTest, MutationsWithoutAnIdentityAreRefused) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);
  auto stub = proto::Kv::NewStub(
      grpc::CreateChannel(nodes_.at(leader).address, grpc::InsecureChannelCredentials()));

  proto::PutRequest req;
  req.set_key("k");
  req.set_value("v");  // no client_id, no seq
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  proto::KvResponse resp;
  ASSERT_TRUE(stub->Put(&ctx, req, &resp).ok());
  EXPECT_EQ(resp.code(), proto::KvResponse::INVALID)
      << "an unidentifiable write could be retried into a double apply";
}

TEST_F(KvClientTest, TheClientSurvivesLosingTheLeaderMidWorkload) {
  StartCluster({1, 2, 3});
  const uint64_t leader = WaitForLeader();
  ASSERT_NE(leader, 0U);

  // Only the two nodes that will survive. A client pointed at a dead address
  // would also work, just more slowly; this keeps the test about failover.
  std::vector<std::string> survivors;
  for (const auto& [id, node] : nodes_) {
    if (id != leader) {
      survivors.push_back(node.address);
    }
  }
  KvClient client(Options(AllAddresses()));

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(client.Put("k" + std::to_string(i), "v" + std::to_string(i)).IsOk());
  }

  CrashNode(leader);

  // These must ride out the election.
  for (int i = 10; i < 20; ++i) {
    const Status s = client.Put("k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_TRUE(s.IsOk()) << "write " << i << " failed after failover: " << s.ToString();
  }

  for (int i = 0; i < 20; ++i) {
    auto got = client.Get("k" + std::to_string(i));
    ASSERT_TRUE(got.IsOk()) << got.GetStatus().ToString();
    ASSERT_TRUE(got->has_value()) << "lost k" << i << " across the failover";
    EXPECT_EQ(**got, "v" + std::to_string(i));
  }
  EXPECT_GE(client.RetryCount(), 1U) << "the failover should have cost at least one retry";
}

// Each thread owns a client (a client serialises its own operations) and checks
// that a read issued after its own acknowledged write sees it. That is
// read-your-writes, a consequence of linearizability that is cheap to assert
// and fails immediately if a read is served from a stale point.
TEST_F(KvClientTest, ConcurrentClientsAlwaysReadTheirOwnWrites) {
  election_tick_ = 60;  // see the note on election_tick_ in the fixture
  StartCluster({1, 2, 3});
  ASSERT_NE(WaitForLeader(), 0U);

  constexpr int kClients = 4;
  constexpr int kOps = 25;
  std::atomic<int> violations{0};
  std::atomic<int> errors{0};
  std::mutex note_mutex;
  std::string first_note;

  std::vector<std::thread> threads;
  threads.reserve(kClients);
  for (int c = 0; c < kClients; ++c) {
    threads.emplace_back([&, c] {
      ClientOptions o = Options(AllAddresses(), "worker" + std::to_string(c));
      o.jitter_seed = 100 + static_cast<uint64_t>(c);
      KvClient client(o);
      const std::string key = "shared-" + std::to_string(c % 2);  // contended on purpose
      for (int i = 0; i < kOps; ++i) {
        const std::string value = "w" + std::to_string(c) + "-" + std::to_string(i);
        const Status put = client.Put(key, value);
        if (!put.IsOk()) {
          if (errors.fetch_add(1) == 0) {
            const std::lock_guard<std::mutex> lock(note_mutex);
            first_note = "put: " + put.ToString();
          }
          continue;
        }
        auto got = client.Get(key);
        if (!got.IsOk()) {
          if (errors.fetch_add(1) == 0) {
            const std::lock_guard<std::mutex> lock(note_mutex);
            first_note = "get: " + got.GetStatus().ToString();
          }
          continue;
        }
        // Another client may have overwritten the key since, so the value can
        // legitimately differ. What cannot happen is the key being ABSENT: this
        // client's own write completed before the read began.
        if (!got->has_value()) {
          ++violations;
        }
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(errors.load(), 0) << "operations failed; the first: " << first_note;
  EXPECT_EQ(violations.load(), 0) << "a read missed a write that had already been acknowledged";
}

}  // namespace
}  // namespace raftkv::client
