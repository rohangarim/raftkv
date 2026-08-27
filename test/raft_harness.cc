#include "raft_harness.h"

namespace raftkv::raft::testing {
namespace {

// A failure message that says only "these two bytes differ" costs an hour of
// re-deriving state that the harness already had. Dump the whole log.
std::string FormatLog(const RaftNode& node) {
  std::string out = "node=" + std::to_string(node.Id()) + " role=" + RoleName(node.role()) +
                    " term=" + std::to_string(node.Term()) +
                    " commit=" + std::to_string(node.CommitIndex()) + " log=[";
  const RaftLog& log = node.Log();
  for (uint64_t i = log.FirstIndex(); i <= log.LastIndex(); ++i) {
    const proto::Entry* e = log.At(i);
    if (e == nullptr) {
      continue;
    }
    if (i != log.FirstIndex()) {
      out += " ";
    }
    out += std::to_string(e->index()) + ":t" + std::to_string(e->term());
    if (e->type() == proto::ENTRY_NOOP) {
      out += ":noop";
    } else if (!e->data().empty()) {
      out += ":" + e->data();
    }
  }
  out += "]";
  return out;
}

}  // namespace

void Cluster::CheckInvariants() {
  const std::string where = " [seed=" + std::to_string(seed_) + "]";

  // ---- Election Safety ---------------------------------------------------
  // At most one leader per term. Two leaders in one term means two nodes each
  // believe they hold a quorum's votes, which is a broken voting rule.
  for (const auto& [id, node] : nodes_) {
    if (node->role() != Role::kLeader) {
      continue;
    }
    const uint64_t term = node->Term();
    auto it = leader_by_term_.find(term);
    if (it == leader_by_term_.end()) {
      leader_by_term_[term] = id;
    } else {
      ASSERT_EQ(it->second, id) << "Election Safety: nodes " << it->second << " and " << id
                                << " are both leader for term " << term << where;
    }
  }

  // ---- Leader Append-Only ------------------------------------------------
  // A leader never overwrites or deletes entries in its own log; it only
  // appends. Violating this means a leader discarded something it may already
  // have told a client was committed.
  for (const auto& [id, node] : nodes_) {
    if (node->role() != Role::kLeader) {
      last_leader_log_.erase(id);
      continue;
    }
    std::vector<std::string> current;
    for (uint64_t i = node->Log().FirstIndex(); i <= node->Log().LastIndex(); ++i) {
      const proto::Entry* e = node->Log().At(i);
      current.push_back(e != nullptr ? e->SerializeAsString() : std::string());
    }
    auto previous = last_leader_log_.find(id);
    if (previous != last_leader_log_.end()) {
      const auto& before = previous->second;
      ASSERT_GE(current.size(), before.size())
          << "Leader Append-Only: leader " << id << " shortened its log" << where;
      for (size_t i = 0; i < before.size(); ++i) {
        ASSERT_EQ(before[i], current[i])
            << "Leader Append-Only: leader " << id << " rewrote entry at offset " << i << where;
      }
    }
    last_leader_log_[id] = std::move(current);
  }

  // ---- Log Matching ------------------------------------------------------
  // If two logs hold an entry with the same index AND term, the entries are
  // identical and so is every entry before them. Checking only the entry
  // itself would miss the prefix half, which is the half that matters.
  for (auto a = nodes_.begin(); a != nodes_.end(); ++a) {
    for (auto b = std::next(a); b != nodes_.end(); ++b) {
      const RaftLog& log_a = a->second->Log();
      const RaftLog& log_b = b->second->Log();
      const uint64_t lo = std::max(log_a.FirstIndex(), log_b.FirstIndex());
      const uint64_t hi = std::min(log_a.LastIndex(), log_b.LastIndex());

      uint64_t highest_match = 0;
      for (uint64_t i = lo; i <= hi; ++i) {
        const proto::Entry* ea = log_a.At(i);
        const proto::Entry* eb = log_b.At(i);
        if (ea == nullptr || eb == nullptr) {
          continue;
        }
        if (ea->term() == eb->term()) {
          ASSERT_EQ(ea->SerializeAsString(), eb->SerializeAsString())
              << "Log Matching: nodes " << a->first << " and " << b->first << " differ at index "
              << i << " despite equal terms" << where;
          highest_match = i;
        }
      }
      // The prefix property.
      for (uint64_t i = lo; i < highest_match; ++i) {
        const proto::Entry* ea = log_a.At(i);
        const proto::Entry* eb = log_b.At(i);
        if (ea != nullptr && eb != nullptr) {
          ASSERT_EQ(ea->SerializeAsString(), eb->SerializeAsString())
              << "Log Matching: prefix below matching index " << highest_match
              << " differs at index " << i << where << "\n  A: " << FormatLog(*a->second)
              << "\n  B: " << FormatLog(*b->second);
        }
      }
    }
  }

  // ---- Leader Completeness ----------------------------------------------
  // A leader holds every entry committed in any earlier term. This is what
  // makes an election safe to serve reads and writes from.
  for (const auto& [id, node] : nodes_) {
    if (node->role() != Role::kLeader) {
      continue;
    }
    for (const auto& [other_id, other] : nodes_) {
      if (other_id == id) {
        continue;
      }
      const uint64_t committed = other->CommitIndex();
      if (committed == 0 || other->Term() > node->Term()) {
        continue;
      }
      for (uint64_t i = std::max<uint64_t>(1, other->Log().FirstIndex()); i <= committed; ++i) {
        const proto::Entry* theirs = other->Log().At(i);
        if (theirs == nullptr || i > node->Log().LastIndex()) {
          continue;
        }
        const proto::Entry* ours = node->Log().At(i);
        if (ours == nullptr) {
          continue;
        }
        ASSERT_EQ(ours->term(), theirs->term())
            << "Leader Completeness: leader " << id << " lacks committed entry " << i
            << " held by node " << other_id << where;
      }
    }
  }

  // ---- State Machine Safety ---------------------------------------------
  // No two nodes apply different commands at the same log index. This is the
  // property a client ultimately depends on, and the one the Phase 10
  // linearizability checker would otherwise discover the hard way.
  std::map<uint64_t, std::pair<uint64_t, std::string>> applied_at;
  for (const auto& [id, entries] : applied_) {
    for (const auto& [index, payload] : entries) {
      auto it = applied_at.find(index);
      if (it == applied_at.end()) {
        applied_at[index] = {id, payload};
      } else {
        ASSERT_EQ(it->second.second, payload)
            << "State Machine Safety: nodes " << it->second.first << " and " << id
            << " applied different commands at index " << index << where;
      }
    }
  }
}

}  // namespace raftkv::raft::testing
