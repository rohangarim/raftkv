#include "lsm/memtable.h"

namespace raftkv::lsm {

void MemTable::Add(SequenceNumber seq, ValueType type, std::string_view key,
                   std::string_view value) {
  std::string internal = MakeInternalKey(key, seq, type);
  const size_t added = internal.size() + value.size() + sizeof(void*) * 4;

  auto [it, inserted] = table_.insert_or_assign(std::move(internal), std::string(value));
  if (inserted) {
    memory_usage_ += added;
  }
}

std::optional<ValueType> MemTable::Get(std::string_view key, SequenceNumber snapshot,
                                       std::string* value) const {
  // Seek to the newest version of `key` visible at `snapshot`. Because entries
  // sort by sequence descending, the first entry at or after this probe is
  // exactly that version -- if one exists for this user key at all.
  const std::string probe = MakeInternalKey(key, snapshot, ValueType::kValue);

  auto it = table_.lower_bound(probe);
  if (it == table_.end()) {
    return std::nullopt;
  }
  if (UserKeyOf(it->first) != key) {
    return std::nullopt;  // walked past into a different key
  }

  const ValueType type = TypeOf(PackedOf(it->first));
  if (type == ValueType::kValue) {
    value->assign(it->second);
  }
  return type;
}

}  // namespace raftkv::lsm
