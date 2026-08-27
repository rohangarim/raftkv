#include "raft/raft_log.h"

#include <algorithm>

namespace raftkv::raft {

uint64_t RaftLog::LastIndex() const { return entries_.empty() ? offset_ : entries_.back().index(); }

Result<uint64_t> RaftLog::TermAt(uint64_t index) const {
  if (index == 0) {
    return uint64_t{0};
  }
  if (index == offset_) {
    return offset_term_;
  }
  if (index < FirstIndex() || index > LastIndex()) {
    return Status::NotFound("log index " + std::to_string(index) + " is not available");
  }
  return entries_[index - FirstIndex()].term();
}

const proto::Entry* RaftLog::At(uint64_t index) const {
  if (index < FirstIndex() || index > LastIndex()) {
    return nullptr;
  }
  return &entries_[index - FirstIndex()];
}

std::vector<proto::Entry> RaftLog::Slice(uint64_t lo, uint64_t hi) const {
  std::vector<proto::Entry> out;
  lo = std::max(lo, FirstIndex());
  hi = std::min(hi, LastIndex());
  if (lo > hi) {
    return out;
  }
  out.reserve(hi - lo + 1);
  for (uint64_t i = lo; i <= hi; ++i) {
    out.push_back(entries_[i - FirstIndex()]);
  }
  return out;
}

Status RaftLog::Append(std::span<const proto::Entry> entries) {
  for (const auto& entry : entries) {
    if (entry.index() != LastIndex() + 1) {
      return Status::InvalidArgument("append at index " + std::to_string(entry.index()) +
                                     " leaves a hole after " + std::to_string(LastIndex()));
    }
    entries_.push_back(entry);
  }
  return Status::Ok();
}

Status RaftLog::TruncateAndAppend(uint64_t prev_index, std::span<const proto::Entry> entries,
                                  uint64_t* first_changed) {
  if (first_changed != nullptr) {
    *first_changed = 0;
  }
  if (prev_index > LastIndex()) {
    return Status::InvalidArgument("append past the end of the log");
  }

  for (size_t i = 0; i < entries.size(); ++i) {
    const proto::Entry& incoming = entries[i];
    const uint64_t index = prev_index + 1 + i;
    if (incoming.index() != index) {
      return Status::InvalidArgument("entry index does not follow prev_index");
    }

    if (index > LastIndex()) {
      // Past the end: everything from here on is new.
      if (first_changed != nullptr) {
        *first_changed = index;
      }
      return Append(entries.subspan(i));
    }

    const proto::Entry* existing = At(index);
    if (existing != nullptr && existing->term() == incoming.term()) {
      continue;  // already have it; leave it alone
    }

    // Conflict: same index, different term. Drop this entry and everything
    // after it, then take the leader's version.
    if (first_changed != nullptr) {
      *first_changed = index;
    }
    entries_.resize(index - FirstIndex());
    return Append(entries.subspan(i));
  }
  return Status::Ok();
}

bool RaftLog::IsUpToDate(uint64_t candidate_last_term, uint64_t candidate_last_index) const {
  auto own_term = TermAt(LastIndex());
  const uint64_t last_term = own_term.IsOk() ? *own_term : offset_term_;
  if (candidate_last_term != last_term) {
    return candidate_last_term > last_term;
  }
  return candidate_last_index >= LastIndex();
}

void RaftLog::CompactTo(uint64_t index, uint64_t index_term) {
  if (index <= offset_) {
    return;
  }
  const uint64_t drop = std::min<uint64_t>(index - offset_, entries_.size());
  entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(drop));
  offset_ = index;
  offset_term_ = index_term;
}

}  // namespace raftkv::raft
