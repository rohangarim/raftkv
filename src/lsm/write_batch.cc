#include "lsm/write_batch.h"

#include <utility>

#include "lsm/coding.h"

namespace raftkv::lsm {

WriteBatch::WriteBatch() { rep_.resize(kHeaderSize, '\0'); }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeaderSize, '\0');
}

uint32_t WriteBatch::Count() const { return DecodeFixed32(rep_.data() + 8); }

void WriteBatch::SetCount(uint32_t count) {
  rep_[8] = static_cast<char>(count & 0xFF);
  rep_[9] = static_cast<char>((count >> 8) & 0xFF);
  rep_[10] = static_cast<char>((count >> 16) & 0xFF);
  rep_[11] = static_cast<char>((count >> 24) & 0xFF);
}

SequenceNumber WriteBatch::Sequence() const { return DecodeFixed64(rep_.data()); }

void WriteBatch::SetSequence(SequenceNumber seq) {
  for (int i = 0; i < 8; ++i) {
    rep_[static_cast<size_t>(i)] = static_cast<char>((seq >> (8 * i)) & 0xFF);
  }
}

void WriteBatch::Put(std::string_view key, std::string_view value) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<char>(ValueType::kValue));
  PutLengthPrefixed(&rep_, key);
  PutLengthPrefixed(&rep_, value);
}

void WriteBatch::Delete(std::string_view key) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<char>(ValueType::kDeletion));
  PutLengthPrefixed(&rep_, key);
}

Status WriteBatch::Iterate(Handler* handler) const {
  if (rep_.size() < kHeaderSize) {
    return Status::Corruption("write batch shorter than its header");
  }

  std::string_view input(rep_);
  input.remove_prefix(kHeaderSize);

  const SequenceNumber base = Sequence();
  const uint32_t expected = Count();
  uint32_t seen = 0;

  while (!input.empty()) {
    const auto type = static_cast<ValueType>(input.front());
    input.remove_prefix(1);

    std::string_view key;
    if (!GetLengthPrefixed(&input, &key)) {
      return Status::Corruption("write batch: truncated key");
    }

    switch (type) {
      case ValueType::kValue: {
        std::string_view value;
        if (!GetLengthPrefixed(&input, &value)) {
          return Status::Corruption("write batch: truncated value");
        }
        handler->Put(base + seen, key, value);
        break;
      }
      case ValueType::kDeletion:
        handler->Delete(base + seen, key);
        break;
      default:
        return Status::Corruption("write batch: unknown record type");
    }
    ++seen;
  }

  if (seen != expected) {
    return Status::Corruption("write batch: count mismatch");
  }
  return Status::Ok();
}

Status WriteBatch::FromContents(std::string contents, WriteBatch* out) {
  if (contents.size() < kHeaderSize) {
    return Status::Corruption("write batch shorter than its header");
  }
  out->rep_ = std::move(contents);

  // Validate before handing it back. A batch that only fails at replay time
  // would surface the corruption in the middle of recovery, which is the
  // worst possible moment to discover it.
  struct NullHandler : Handler {
    void Put(SequenceNumber, std::string_view, std::string_view) override {}
    void Delete(SequenceNumber, std::string_view) override {}
  } null_handler;
  return out->Iterate(&null_handler);
}

}  // namespace raftkv::lsm
