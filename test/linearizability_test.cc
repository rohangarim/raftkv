// Unit tests for the checker itself. A checker that accepts everything would
// make the cluster campaign meaningless, so these pin down both directions:
// histories it must accept and histories it must reject.

#include "linearizability.h"

#include <gtest/gtest.h>

namespace raftkv::testing {
namespace {

HistoryOp Put(const std::string& key, const std::string& v, uint64_t invoke, uint64_t ret) {
  HistoryOp o;
  o.kind = RegOp::kPut;
  o.key = key;
  o.value = v;
  o.invoke = invoke;
  o.ret = ret;
  return o;
}

HistoryOp Get(const std::string& key, std::optional<std::string> seen, uint64_t invoke, uint64_t ret) {
  HistoryOp o;
  o.kind = RegOp::kGet;
  o.key = key;
  o.observed = std::move(seen);
  o.invoke = invoke;
  o.ret = ret;
  return o;
}

HistoryOp Cas(const std::string& key, std::optional<std::string> expected, const std::string& to,
              bool applied, std::optional<std::string> found, uint64_t invoke, uint64_t ret) {
  HistoryOp o;
  o.kind = RegOp::kCas;
  o.key = key;
  o.expected = std::move(expected);
  o.value = to;
  o.applied = applied;
  o.observed = std::move(found);
  o.invoke = invoke;
  o.ret = ret;
  return o;
}

HistoryOp Unknown(HistoryOp op) {
  op.completed = false;
  op.ret = std::numeric_limits<uint64_t>::max();
  return op;
}

TEST(Linearizability, SequentialHistoryIsAccepted) {
  EXPECT_TRUE(CheckLinearizable({Get("k", std::nullopt, 1, 2), Put("k", "a", 3, 4), Get("k", "a", 5, 6),
                                 Put("k", "b", 7, 8), Get("k", "b", 9, 10)})
                  .linearizable);
}

TEST(Linearizability, ReadingAnOverwrittenValueIsRejected) {
  const auto r = CheckLinearizable({Put("k", "a", 1, 2), Put("k", "b", 3, 4), Get("k", "a", 5, 6)});
  EXPECT_FALSE(r.linearizable);
  EXPECT_EQ(r.failing_key, "k");
}

TEST(Linearizability, ReadingAValueNeverWrittenIsRejected) {
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 2), Get("k", "ghost", 3, 4)}).linearizable);
}

TEST(Linearizability, ConcurrentReadMaySeeEitherSide) {
  // The read overlaps the write, so both outcomes are legal.
  EXPECT_TRUE(CheckLinearizable({Put("k", "a", 1, 10), Get("k", std::nullopt, 2, 3)}).linearizable);
  EXPECT_TRUE(CheckLinearizable({Put("k", "a", 1, 10), Get("k", "a", 2, 3)}).linearizable);
}

TEST(Linearizability, ReadsMayNotGoBackInTime) {
  // One reader sees the new value, a later reader sees the old: not allowed
  // even though each read alone overlaps the write.
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 100), Get("k", "a", 2, 3), Get("k", std::nullopt, 4, 5)})
                   .linearizable);
}

TEST(Linearizability, AWriteThatReturnedCannotBeLost) {
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 2), Get("k", std::nullopt, 3, 4)}).linearizable);
}

TEST(Linearizability, KeysAreIndependent) {
  EXPECT_TRUE(CheckLinearizable({Put("x", "1", 1, 2), Get("y", std::nullopt, 3, 4), Get("x", "1", 5, 6)})
                  .linearizable);
}

TEST(Linearizability, AnUnknownWriteMayHaveTakenEffectOrNot) {
  const HistoryOp maybe = Unknown(Put("k", "a", 1, 0));
  EXPECT_TRUE(CheckLinearizable({maybe, Get("k", "a", 5, 6)}).linearizable);
  EXPECT_TRUE(CheckLinearizable({maybe, Get("k", std::nullopt, 5, 6)}).linearizable);
}

TEST(Linearizability, AnUnknownWriteCannotExplainAValueFromBeforeItWasInvoked) {
  EXPECT_FALSE(CheckLinearizable({Get("k", "a", 1, 2), Unknown(Put("k", "a", 5, 0))}).linearizable);
}

TEST(Linearizability, AnUnknownWriteTakesEffectAtMostOnce) {
  // Seeing a, then b, then a again would need the same write twice.
  EXPECT_FALSE(CheckLinearizable({Unknown(Put("k", "a", 1, 0)), Put("k", "b", 2, 3), Get("k", "a", 4, 5),
                                  Get("k", "b", 6, 7), Get("k", "a", 8, 9)})
                   .linearizable);
}

TEST(Linearizability, CompareAndSwapObeysItsPrecondition) {
  EXPECT_TRUE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "a", "b", true, std::nullopt, 3, 4),
                                 Get("k", "b", 5, 6)})
                  .linearizable);
  // A failed swap must report what it found.
  EXPECT_TRUE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "z", "b", false, "a", 3, 4)}).linearizable);
  // A swap cannot succeed against the wrong value.
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "z", "b", true, std::nullopt, 3, 4)})
                   .linearizable);
  // Nor fail against the right one.
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "a", "b", false, "a", 3, 4)}).linearizable);
}

TEST(Linearizability, TwoConcurrentSwapsCannotBothWin) {
  EXPECT_FALSE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "a", "b", true, std::nullopt, 3, 10),
                                  Cas("k", "a", "c", true, std::nullopt, 4, 11)})
                   .linearizable);
  EXPECT_TRUE(CheckLinearizable({Put("k", "a", 1, 2), Cas("k", "a", "b", true, std::nullopt, 3, 10),
                                 Cas("k", "a", "c", false, "b", 4, 11)})
                  .linearizable);
}

TEST(Linearizability, ManyOverlappingWritersStayTractable) {
  std::vector<HistoryOp> h;
  h.reserve(13);
  uint64_t t = 1;
  for (int i = 0; i < 12; ++i) h.push_back(Put("k", "v" + std::to_string(i), t++, 1000 + i));
  h.push_back(Get("k", "v11", 2000, 2001));
  const auto r = CheckLinearizable(h);
  EXPECT_TRUE(r.linearizable);
  EXPECT_FALSE(r.inconclusive);
}

}  // namespace
}  // namespace raftkv::testing
