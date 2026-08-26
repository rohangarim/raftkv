#pragma once

// Internal key encoding.
//
// An internal key is: user_key || little-endian u64 (sequence << 8 | type).
//
// Records sort by user key ascending, then by sequence DESCENDING, so the
// newest version of a key is the first one encountered during a scan. That
// single ordering choice is what makes snapshots nearly free: a read at
// snapshot sequence S walks forward from the first record for a user key and
// takes the first version whose sequence is <= S. No version chains, no
// separate index.
//
// Deletes are tombstones (kTypeDeletion) rather than removals, because an
// older SSTable may still hold the value; the tombstone is what shadows it
// until compaction drops both.

#include <cstdint>
#include <string>
#include <string_view>

#include "lsm/coding.h"

namespace raftkv::lsm {

using SequenceNumber = uint64_t;

// The top 8 bits of the packed u64 are the type, so sequence numbers are
// limited to 56 bits. At a million writes per second that is over two
// thousand years, which is a limit we can live with.
constexpr SequenceNumber kMaxSequenceNumber = (1ULL << 56) - 1;

enum class ValueType : uint8_t {
  kDeletion = 0,
  kValue = 1,
};

// kValue sorts after kDeletion, so when a Put and a Delete share a sequence
// number -- which cannot happen through the public API, but can be
// constructed by a corrupt file -- the ordering is still total and defined.
constexpr uint64_t PackSequenceAndType(SequenceNumber seq, ValueType type) {
  return (seq << 8) | static_cast<uint64_t>(type);
}

constexpr SequenceNumber SequenceOf(uint64_t packed) { return packed >> 8; }

constexpr ValueType TypeOf(uint64_t packed) {
  return static_cast<ValueType>(packed & 0xFFU);
}

// Builds the internal key for a user key at a given sequence.
inline std::string MakeInternalKey(std::string_view user_key, SequenceNumber seq, ValueType type) {
  std::string out;
  out.reserve(user_key.size() + 8);
  out.append(user_key);
  PutFixed64(&out, PackSequenceAndType(seq, type));
  return out;
}

inline bool IsValidInternalKey(std::string_view internal_key) {
  return internal_key.size() >= 8;
}

inline std::string_view UserKeyOf(std::string_view internal_key) {
  return internal_key.substr(0, internal_key.size() - 8);
}

inline uint64_t PackedOf(std::string_view internal_key) {
  return DecodeFixed64(internal_key.data() + internal_key.size() - 8);
}

// User key ascending, then sequence descending (newest first).
struct InternalKeyComparator {
  using is_transparent = void;

  bool operator()(std::string_view a, std::string_view b) const {
    const std::string_view ua = UserKeyOf(a);
    const std::string_view ub = UserKeyOf(b);
    if (ua != ub) {
      return ua < ub;
    }
    // Descending: a larger packed value (newer sequence) compares "less" so it
    // is visited first.
    return PackedOf(a) > PackedOf(b);
  }
};

}  // namespace raftkv::lsm
