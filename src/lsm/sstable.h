#pragma once

// Immutable on-disk sorted table.
//
// File layout:
//
//   data    repeated { varint klen | internal_key | varint vlen | value }
//           in ascending internal-key order (user key asc, sequence desc)
//   index   repeated { varint klen | internal_key | varint offset }
//           one entry per kIndexInterval data records; `offset` is the file
//           offset of that data record
//   footer  fixed64 index_offset | fixed64 index_size | fixed32 crc | fixed32 magic
//
// The footer is fixed width and lives at the end, so opening a table is one
// small read at a known position rather than a scan. Its CRC covers the two
// offsets: a corrupted index_offset would otherwise send the reader off to
// parse garbage as an index, and the failure would surface as nonsense keys
// rather than as corruption.
//
// The index is sparse. A dense index would be as large as the key set and
// would have to be paged; a sparse one costs a short forward scan per lookup
// and stays in memory. kIndexInterval is the knob between those.
//
// Tables are immutable once finished. That is what makes snapshots cheap: a
// snapshot pins a set of files and a sequence number, and nothing can rewrite
// a file underneath it. Compaction produces new files and deletes old ones
// only when no snapshot still refers to them.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lsm/file.h"
#include "lsm/internal_key.h"
#include "lsm/status.h"

namespace raftkv::lsm {

// One index entry per this many data records.
inline constexpr size_t kIndexInterval = 16;

inline constexpr uint32_t kSsTableMagic = 0x524B5654U;  // "RKVT"
inline constexpr size_t kFooterSize = 24;

class SsTableBuilder {
 public:
  static Result<std::unique_ptr<SsTableBuilder>> Create(const std::filesystem::path& path);
  ~SsTableBuilder();

  SsTableBuilder(const SsTableBuilder&) = delete;
  SsTableBuilder& operator=(const SsTableBuilder&) = delete;

  // Keys must arrive in strictly ascending internal-key order. Violating that
  // is a programming error in the caller (a broken merge), not a data
  // condition, so it is reported rather than silently sorted.
  Status Add(std::string_view internal_key, std::string_view value);

  // Writes index and footer, then fsyncs. The file is not a valid table until
  // this returns Ok.
  Status Finish();

  uint64_t NumEntries() const { return num_entries_; }
  uint64_t FileSize() const { return file_size_; }

 private:
  SsTableBuilder(std::unique_ptr<WritableFile> file, std::filesystem::path path);

  std::unique_ptr<WritableFile> file_;
  std::filesystem::path path_;
  std::string last_key_;
  std::vector<std::pair<std::string, uint64_t>> index_;
  uint64_t num_entries_ = 0;
  uint64_t file_size_ = 0;
  bool finished_ = false;
};

class SsTable {
 public:
  static Result<std::shared_ptr<SsTable>> Open(const std::filesystem::path& path);

  // Point lookup of `user_key` as of `snapshot`.
  //
  // Same three-way result as MemTable::Get, and for the same reason: a
  // tombstone must be distinguishable from an absence, or a delete recorded
  // in a newer table fails to shadow a value in an older one.
  Result<std::optional<ValueType>> Get(std::string_view user_key, SequenceNumber snapshot,
                                       std::string* value) const;

  // Forward cursor over every record, in internal-key order. Used by
  // compaction.
  class Iterator {
   public:
    virtual ~Iterator() = default;
    // Returns kNotFound when exhausted. The returned views are valid until
    // the next call to Next().
    virtual Status Next(std::string_view* internal_key, std::string_view* value) = 0;
  };

  std::unique_ptr<Iterator> NewIterator() const;

  const std::filesystem::path& Path() const { return file_->Path(); }
  uint64_t FileSize() const { return file_->Size(); }
  uint64_t NumIndexEntries() const { return index_.size(); }

 private:
  SsTable(std::unique_ptr<RandomAccessFile> file,
          std::vector<std::pair<std::string, uint64_t>> index, uint64_t data_end);

  // File offset to begin scanning from for `probe`, using the sparse index.
  uint64_t SeekAnchor(std::string_view probe) const;

  std::unique_ptr<RandomAccessFile> file_;
  std::vector<std::pair<std::string, uint64_t>> index_;
  uint64_t data_end_ = 0;
};

}  // namespace raftkv::lsm
