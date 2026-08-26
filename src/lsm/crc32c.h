#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace raftkv::lsm {

// CRC-32C (Castagnoli, polynomial 0x1EDC6F41).
//
// Used for torn-write detection in the WAL and integrity checking in SSTable
// footers. Castagnoli rather than the zlib polynomial because it has hardware
// support on both x86 (SSE4.2) and ARMv8, and because it has better error
// detection for the short records a WAL produces.
//
// This is a table-driven software implementation. It is not the fastest
// possible; if CRC shows up in a Phase 9 profile we replace it with the
// hardware intrinsic and say so in DECISIONS.md rather than assuming.
uint32_t Crc32c(const void* data, size_t size);

inline uint32_t Crc32c(std::string_view s) { return Crc32c(s.data(), s.size()); }

}  // namespace raftkv::lsm
