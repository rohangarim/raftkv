#include <limits>
#include <string>

#include <gtest/gtest.h>

#include "lsm/coding.h"
#include "lsm/crc32c.h"
#include "lsm/internal_key.h"

namespace raftkv::lsm {
namespace {

TEST(CodingTest, Fixed32RoundTrips) {
  for (uint32_t v : {0U, 1U, 255U, 256U, 0x12345678U, std::numeric_limits<uint32_t>::max()}) {
    std::string s;
    PutFixed32(&s, v);
    ASSERT_EQ(s.size(), 4U);
    EXPECT_EQ(DecodeFixed32(s.data()), v);
  }
}

TEST(CodingTest, Fixed64RoundTrips) {
  for (uint64_t v : {0ULL, 1ULL, 1ULL << 40, std::numeric_limits<uint64_t>::max()}) {
    std::string s;
    PutFixed64(&s, v);
    ASSERT_EQ(s.size(), 8U);
    EXPECT_EQ(DecodeFixed64(s.data()), v);
  }
}

// Encoding is little-endian on the wire regardless of host byte order,
// because replicas must agree byte for byte.
TEST(CodingTest, Fixed32IsLittleEndianOnTheWire) {
  std::string s;
  PutFixed32(&s, 0x01020304U);
  EXPECT_EQ(static_cast<uint8_t>(s[0]), 0x04);
  EXPECT_EQ(static_cast<uint8_t>(s[3]), 0x01);
}

TEST(CodingTest, VarintRoundTrips) {
  const uint64_t values[] = {0,   1,          127,        128,
                             300, 1ULL << 20, 1ULL << 62, std::numeric_limits<uint64_t>::max()};
  std::string s;
  for (uint64_t v : values) {
    PutVarint64(&s, v);
  }
  std::string_view in(s);
  for (uint64_t expected : values) {
    uint64_t got = 0;
    ASSERT_TRUE(GetVarint64(&in, &got));
    EXPECT_EQ(got, expected);
  }
  EXPECT_TRUE(in.empty());
}

TEST(CodingTest, VarintRejectsTruncation) {
  std::string s;
  PutVarint64(&s, 1ULL << 40);
  s.pop_back();  // drop the terminating byte
  std::string_view in(s);
  uint64_t got = 0;
  EXPECT_FALSE(GetVarint64(&in, &got));
}

TEST(CodingTest, LengthPrefixedHandlesEmptyAndBinary) {
  const std::string binary("a\0b\xff", 4);
  std::string s;
  PutLengthPrefixed(&s, "");
  PutLengthPrefixed(&s, binary);

  std::string_view in(s);
  std::string_view out;
  ASSERT_TRUE(GetLengthPrefixed(&in, &out));
  EXPECT_EQ(out, "");
  ASSERT_TRUE(GetLengthPrefixed(&in, &out));
  EXPECT_EQ(out, binary);
}

TEST(CodingTest, LengthPrefixedRejectsOverlongLength) {
  std::string s;
  PutVarint64(&s, 100);  // claims 100 bytes
  s.append("short");
  std::string_view in(s);
  std::string_view out;
  EXPECT_FALSE(GetLengthPrefixed(&in, &out));
}

// Known-answer tests. A CRC implementation that is self-consistent but wrong
// passes every round-trip test and fails to interoperate with anything.
TEST(Crc32cTest, MatchesKnownVectors) {
  EXPECT_EQ(Crc32c("", 0), 0x00000000U);
  EXPECT_EQ(Crc32c(std::string_view("a")), 0xC1D04330U);
  EXPECT_EQ(Crc32c(std::string_view("123456789")), 0xE3069283U);
}

TEST(Crc32cTest, DetectsSingleBitFlip) {
  std::string data(64, 'x');
  const uint32_t before = Crc32c(data);
  data[31] = static_cast<char>(data[31] ^ 0x01);
  EXPECT_NE(Crc32c(data), before);
}

TEST(InternalKeyTest, PackAndUnpackRoundTrip) {
  const auto packed = PackSequenceAndType(123456789, ValueType::kValue);
  EXPECT_EQ(SequenceOf(packed), 123456789U);
  EXPECT_EQ(TypeOf(packed), ValueType::kValue);
}

TEST(InternalKeyTest, ExtractsUserKey) {
  const std::string ik = MakeInternalKey("hello", 42, ValueType::kDeletion);
  EXPECT_EQ(UserKeyOf(ik), "hello");
  EXPECT_EQ(SequenceOf(PackedOf(ik)), 42U);
  EXPECT_EQ(TypeOf(PackedOf(ik)), ValueType::kDeletion);
}

// The ordering the whole snapshot mechanism rests on: user key ascending,
// sequence descending so the newest version is seen first.
TEST(InternalKeyTest, SortsUserKeyAscendingSequenceDescending) {
  InternalKeyComparator cmp;
  const std::string a1 = MakeInternalKey("a", 1, ValueType::kValue);
  const std::string a2 = MakeInternalKey("a", 2, ValueType::kValue);
  const std::string b1 = MakeInternalKey("b", 1, ValueType::kValue);

  EXPECT_TRUE(cmp(a2, a1)) << "newer sequence must sort first";
  EXPECT_FALSE(cmp(a1, a2));
  EXPECT_TRUE(cmp(a1, b1)) << "user key ordering dominates";
  EXPECT_FALSE(cmp(b1, a1));
}

TEST(InternalKeyTest, HandlesUserKeysThatArePrefixesOfOthers) {
  InternalKeyComparator cmp;
  const std::string a = MakeInternalKey("abc", 5, ValueType::kValue);
  const std::string ab = MakeInternalKey("abcd", 5, ValueType::kValue);
  EXPECT_TRUE(cmp(a, ab));
  EXPECT_FALSE(cmp(ab, a));
}

}  // namespace
}  // namespace raftkv::lsm
