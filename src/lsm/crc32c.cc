#include "lsm/crc32c.h"

#include <array>

namespace raftkv::lsm {
namespace {

constexpr uint32_t kPolynomial = 0x82F63B78U;  // reversed 0x1EDC6F41

// Built at compile time so there is no initialization order or thread-safety
// question about a lazily-populated lookup table.
constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc & 1U) != 0U ? (crc >> 1) ^ kPolynomial : crc >> 1;
    }
    table[i] = crc;
  }
  return table;
}

constexpr std::array<uint32_t, 256> kTable = MakeTable();

}  // namespace

uint32_t Crc32c(const void* data, size_t size) {
  const auto* p = static_cast<const uint8_t*>(data);
  uint32_t crc = 0xFFFFFFFFU;
  for (size_t i = 0; i < size; ++i) {
    crc = kTable[(crc ^ p[i]) & 0xFFU] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFU;
}

}  // namespace raftkv::lsm
