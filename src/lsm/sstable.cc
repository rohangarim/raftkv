#include "lsm/sstable.h"

#include <algorithm>
#include <utility>

#include "lsm/coding.h"
#include "lsm/crc32c.h"

namespace raftkv::lsm {
namespace {

constexpr size_t kScanChunk = size_t{32} * 1024;

void AppendRecord(std::string* out, std::string_view key, std::string_view value) {
  PutLengthPrefixed(out, key);
  PutLengthPrefixed(out, value);
}

// Forward cursor over a byte range of the data section.
//
// Records are read in chunks rather than one pread per record, and a record
// that straddles a chunk boundary grows the buffer until it fits. That last
// part is the whole reason this is a class: a naive "read 32 KiB and parse"
// silently drops any record whose encoding crosses the boundary.
class RecordCursor {
 public:
  RecordCursor(const RandomAccessFile* file, uint64_t begin, uint64_t end)
      : file_(file), buffer_start_(begin), end_(end) {}

  Status Next(std::string_view* key, std::string_view* value) {
    // Drop what has been consumed so offsets stay bounded.
    if (consumed_ > 0) {
      buffer_.erase(0, consumed_);
      buffer_start_ += consumed_;
      consumed_ = 0;
    }

    if (buffer_.empty() && buffer_start_ >= end_) {
      return Status::NotFound("end of table");
    }

    while (true) {
      std::string_view input(buffer_);
      std::string_view k;
      std::string_view v;
      if (GetLengthPrefixed(&input, &k) && GetLengthPrefixed(&input, &v)) {
        consumed_ = buffer_.size() - input.size();
        *key = k;
        *value = v;
        return Status::Ok();
      }

      // Not enough buffered. Pull more; if there is no more, the file ends
      // mid-record, which is corruption rather than a clean end.
      const uint64_t buffered_end = buffer_start_ + buffer_.size();
      if (buffered_end >= end_) {
        return Status::Corruption("sstable: record truncated at offset " +
                                  std::to_string(buffer_start_));
      }
      const size_t want = static_cast<size_t>(
          std::min<uint64_t>(std::max<uint64_t>(kScanChunk, buffer_.size()), end_ - buffered_end));
      std::string more;
      RAFTKV_RETURN_IF_ERROR(file_->ReadExactly(buffered_end, want, &more));
      buffer_.append(more);
    }
  }

 private:
  const RandomAccessFile* file_;
  std::string buffer_;
  uint64_t buffer_start_ = 0;
  uint64_t end_ = 0;
  size_t consumed_ = 0;
};

class SsTableIteratorImpl : public SsTable::Iterator {
 public:
  SsTableIteratorImpl(const RandomAccessFile* file, uint64_t begin, uint64_t end)
      : cursor_(file, begin, end) {}

  Status Next(std::string_view* internal_key, std::string_view* value) override {
    return cursor_.Next(internal_key, value);
  }

 private:
  RecordCursor cursor_;
};

}  // namespace

// ---------------------------------------------------------------------------
// SsTableBuilder
// ---------------------------------------------------------------------------

SsTableBuilder::SsTableBuilder(std::unique_ptr<WritableFile> file, std::filesystem::path path)
    : file_(std::move(file)), path_(std::move(path)) {}

SsTableBuilder::~SsTableBuilder() = default;

Result<std::unique_ptr<SsTableBuilder>> SsTableBuilder::Create(const std::filesystem::path& path) {
  auto opened = WritableFile::Create(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<SsTableBuilder> builder(new SsTableBuilder(opened.TakeValue(), path));
  return builder;
}

Status SsTableBuilder::Add(std::string_view internal_key, std::string_view value) {
  if (finished_) {
    return Status::InvalidArgument("Add after Finish");
  }
  if (!IsValidInternalKey(internal_key)) {
    return Status::InvalidArgument("internal key shorter than its tag");
  }

  // Strictly ascending. A merge that emits keys out of order produces a table
  // whose binary search silently returns wrong answers, so this is checked
  // rather than assumed.
  if (num_entries_ > 0) {
    const InternalKeyComparator cmp;
    if (!cmp(last_key_, internal_key)) {
      return Status::InvalidArgument("sstable keys must be strictly ascending");
    }
  }

  const uint64_t offset = file_->Offset();
  if (num_entries_ % kIndexInterval == 0) {
    index_.emplace_back(std::string(internal_key), offset);
  }

  std::string record;
  AppendRecord(&record, internal_key, value);
  RAFTKV_RETURN_IF_ERROR(file_->Append(record));

  last_key_.assign(internal_key);
  max_sequence_ = std::max(max_sequence_, SequenceOf(PackedOf(internal_key)));
  ++num_entries_;
  return Status::Ok();
}

Status SsTableBuilder::Finish() {
  if (finished_) {
    return Status::InvalidArgument("Finish called twice");
  }
  finished_ = true;

  const uint64_t index_offset = file_->Offset();

  std::string index_bytes;
  for (const auto& [key, offset] : index_) {
    PutLengthPrefixed(&index_bytes, key);
    PutVarint64(&index_bytes, offset);
  }
  RAFTKV_RETURN_IF_ERROR(file_->Append(index_bytes));

  std::string footer;
  PutFixed64(&footer, index_offset);
  PutFixed64(&footer, index_bytes.size());
  PutFixed64(&footer, max_sequence_);
  PutFixed32(&footer, Crc32c(footer.data(), footer.size()));
  PutFixed32(&footer, kSsTableMagic);
  RAFTKV_RETURN_IF_ERROR(file_->Append(footer));

  // Sync before the file is considered a table. A caller that deletes WAL
  // records because "the table is written" must be able to rely on that.
  RAFTKV_RETURN_IF_ERROR(file_->Sync());
  file_size_ = file_->Offset();
  return file_->Close();
}

// ---------------------------------------------------------------------------
// SsTable
// ---------------------------------------------------------------------------

SsTable::SsTable(std::unique_ptr<RandomAccessFile> file,
                 std::vector<std::pair<std::string, uint64_t>> index, uint64_t data_end,
                 SequenceNumber max_sequence)
    : file_(std::move(file)),
      index_(std::move(index)),
      data_end_(data_end),
      max_sequence_(max_sequence) {}

Result<std::shared_ptr<SsTable>> SsTable::Open(const std::filesystem::path& path) {
  auto opened = RandomAccessFile::Open(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  std::unique_ptr<RandomAccessFile> file = opened.TakeValue();

  if (file->Size() < kFooterSize) {
    return Status::Corruption("sstable smaller than its footer: " + path.string());
  }

  std::string footer;
  RAFTKV_RETURN_IF_ERROR(file->ReadExactly(file->Size() - kFooterSize, kFooterSize, &footer));

  const uint32_t magic = DecodeFixed32(footer.data() + 28);
  if (magic != kSsTableMagic) {
    return Status::Corruption("sstable bad magic: " + path.string());
  }
  const uint32_t stored_crc = DecodeFixed32(footer.data() + 24);
  if (Crc32c(footer.data(), 24) != stored_crc) {
    return Status::Corruption("sstable footer CRC mismatch: " + path.string());
  }

  const uint64_t index_offset = DecodeFixed64(footer.data());
  const uint64_t index_size = DecodeFixed64(footer.data() + 8);
  const SequenceNumber max_sequence = DecodeFixed64(footer.data() + 16);
  if (index_offset > file->Size() || index_size > file->Size() ||
      index_offset + index_size != file->Size() - kFooterSize) {
    return Status::Corruption("sstable index bounds inconsistent: " + path.string());
  }

  std::string index_bytes;
  if (index_size > 0) {
    RAFTKV_RETURN_IF_ERROR(file->ReadExactly(index_offset, index_size, &index_bytes));
  }

  std::vector<std::pair<std::string, uint64_t>> index;
  std::string_view input(index_bytes);
  while (!input.empty()) {
    std::string_view key;
    uint64_t offset = 0;
    if (!GetLengthPrefixed(&input, &key) || !GetVarint64(&input, &offset)) {
      return Status::Corruption("sstable index truncated: " + path.string());
    }
    if (offset > index_offset) {
      return Status::Corruption("sstable index points past data: " + path.string());
    }
    index.emplace_back(std::string(key), offset);
  }

  std::shared_ptr<SsTable> table(
      new SsTable(std::move(file), std::move(index), index_offset, max_sequence));
  return table;
}

uint64_t SsTable::SeekAnchor(std::string_view probe) const {
  if (index_.empty()) {
    return 0;
  }
  const InternalKeyComparator cmp;
  // Last index entry whose key is < probe. Starting from the entry at or
  // after probe would skip records that live between index anchors.
  auto it = std::lower_bound(index_.begin(), index_.end(), probe,
                             [&cmp](const std::pair<std::string, uint64_t>& entry,
                                    std::string_view value) { return cmp(entry.first, value); });
  if (it == index_.begin()) {
    return index_.front().second;
  }
  --it;
  return it->second;
}

Result<std::optional<ValueType>> SsTable::Get(std::string_view user_key, SequenceNumber snapshot,
                                              std::string* value) const {
  const std::string probe = MakeInternalKey(user_key, snapshot, ValueType::kValue);
  const InternalKeyComparator cmp;

  RecordCursor cursor(file_.get(), SeekAnchor(probe), data_end_);
  while (true) {
    std::string_view key;
    std::string_view val;
    const Status s = cursor.Next(&key, &val);
    if (s.ErrCode() == Code::kNotFound) {
      return std::optional<ValueType>{};
    }
    if (!s.IsOk()) {
      return s;
    }
    if (!IsValidInternalKey(key)) {
      return Status::Corruption("sstable: record key shorter than its tag");
    }

    // Still newer than the snapshot: keep walking.
    if (cmp(key, probe)) {
      continue;
    }
    // First record at or past the probe. If it belongs to another user key,
    // this key has no version visible at the snapshot.
    if (UserKeyOf(key) != user_key) {
      return std::optional<ValueType>{};
    }
    const ValueType type = TypeOf(PackedOf(key));
    if (type == ValueType::kValue) {
      value->assign(val);
    }
    return std::optional<ValueType>{type};
  }
}

std::unique_ptr<SsTable::Iterator> SsTable::NewIterator() const {
  return std::make_unique<SsTableIteratorImpl>(file_.get(), 0, data_end_);
}

}  // namespace raftkv::lsm
