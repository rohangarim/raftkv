#pragma once

// Fixed-width and varint encoding helpers.
//
// Everything here is explicitly little-endian on the wire regardless of host
// byte order. The state machine must be deterministic across replicas, and
// "we only ever run on x86" is the kind of assumption that becomes a
// corruption bug on the day someone runs a follower on an ARM box.

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>

namespace raftkv::lsm {

inline void PutFixed32(std::string* dst, uint32_t value) {
  char buf[4];
  buf[0] = static_cast<char>(value & 0xFF);
  buf[1] = static_cast<char>((value >> 8) & 0xFF);
  buf[2] = static_cast<char>((value >> 16) & 0xFF);
  buf[3] = static_cast<char>((value >> 24) & 0xFF);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
  char buf[8];
  for (int i = 0; i < 8; ++i) {
    buf[i] = static_cast<char>((value >> (8 * i)) & 0xFF);
  }
  dst->append(buf, sizeof(buf));
}

inline uint32_t DecodeFixed32(const char* p) {
  const auto* u = reinterpret_cast<const uint8_t*>(p);
  return static_cast<uint32_t>(u[0]) | (static_cast<uint32_t>(u[1]) << 8) |
         (static_cast<uint32_t>(u[2]) << 16) | (static_cast<uint32_t>(u[3]) << 24);
}

inline uint64_t DecodeFixed64(const char* p) {
  const auto* u = reinterpret_cast<const uint8_t*>(p);
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(u[i]) << (8 * i);
  }
  return value;
}

inline void PutVarint64(std::string* dst, uint64_t value) {
  while (value >= 0x80) {
    dst->push_back(static_cast<char>(value | 0x80));
    value >>= 7;
  }
  dst->push_back(static_cast<char>(value));
}

// Decodes a varint from `input`, advancing it past the bytes consumed.
// Returns false on truncation or on an over-long encoding, which is what a
// corrupt or hostile file looks like.
inline bool GetVarint64(std::string_view* input, uint64_t* value) {
  uint64_t result = 0;
  for (size_t shift = 0; shift <= 63; shift += 7) {
    if (input->empty()) {
      return false;
    }
    const auto byte = static_cast<uint8_t>(input->front());
    input->remove_prefix(1);
    result |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      *value = result;
      return true;
    }
  }
  return false;
}

// Length-prefixed byte string.
inline void PutLengthPrefixed(std::string* dst, std::string_view value) {
  PutVarint64(dst, value.size());
  dst->append(value);
}

inline bool GetLengthPrefixed(std::string_view* input, std::string_view* value) {
  uint64_t len = 0;
  if (!GetVarint64(input, &len)) {
    return false;
  }
  if (input->size() < len) {
    return false;
  }
  *value = input->substr(0, len);
  input->remove_prefix(len);
  return true;
}

}  // namespace raftkv::lsm
