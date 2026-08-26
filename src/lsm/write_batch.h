#pragma once

// A set of mutations that become visible together or not at all.
//
// This is the primitive the Raft state machine is built on. Phase 1b writes
// the user's mutation and the new last_applied_index into the SAME batch, so
// recovery can never observe an effect without its index or an index without
// its effect. If the process dies mid-write, the WAL's CRC check discards the
// partial record whole and both roll back together.
//
// Wire format:
//   sequence : fixed64   (assigned by DB::Write, zero until then)
//   count    : fixed32   (number of records)
//   records  : repeated
//                type : u8  (ValueType)
//                key  : length-prefixed
//                value: length-prefixed, present only for kValue

#include <cstdint>
#include <string>
#include <string_view>

#include "lsm/internal_key.h"
#include "lsm/status.h"

namespace raftkv::lsm {

class WriteBatch {
 public:
  WriteBatch();

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);

  void Clear();

  uint32_t Count() const;
  size_t ApproximateSize() const { return rep_.size(); }
  bool Empty() const { return Count() == 0; }

  SequenceNumber Sequence() const;
  void SetSequence(SequenceNumber seq);

  // The serialized form, which is exactly what goes into one WAL record.
  const std::string& Contents() const { return rep_; }

  // Replays the batch. Called with each record's sequence number already
  // offset by its position, so a batch of N records consumes N sequence
  // numbers and every record is individually addressable by a snapshot.
  class Handler {
   public:
    virtual ~Handler() = default;
    virtual void Put(SequenceNumber seq, std::string_view key, std::string_view value) = 0;
    virtual void Delete(SequenceNumber seq, std::string_view key) = 0;
  };

  Status Iterate(Handler* handler) const;

  // Rebuilds a batch from bytes read back out of the WAL. Rejects anything
  // malformed rather than trusting the file.
  static Status FromContents(std::string contents, WriteBatch* out);

 private:
  static constexpr size_t kHeaderSize = 12;  // fixed64 sequence + fixed32 count

  void SetCount(uint32_t count);

  std::string rep_;
};

}  // namespace raftkv::lsm
