#include "raft/raft_node.h"

#include <algorithm>
#include <utility>

#include "common/log.h"

namespace raftkv::raft {

const char* RoleName(Role role) {
  switch (role) {
    case Role::kFollower:
      return "follower";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "?";
}

RaftNode::RaftNode(Config config) : config_(std::move(config)), rng_(config_.random_seed) {
  for (uint64_t peer : config_.peers) {
    progress_[peer] = PeerProgress{};
  }
  last_hard_state_.set_term(0);
  last_hard_state_.set_vote(0);
  last_hard_state_.set_commit(0);
  ResetElectionTimer();
}

void RaftNode::RestoreHardState(const proto::HardState& hard_state) {
  term_ = hard_state.term();
  vote_ = hard_state.vote();
  commit_index_ = hard_state.commit();
  last_hard_state_ = hard_state;
  hard_state_dirty_ = false;
}

Status RaftNode::RestoreLog(std::span<const proto::Entry> entries) {
  RAFTKV_RETURN_IF_ERROR(log_.Append(entries));
  persisted_index_ = log_.LastIndex();
  return Status::Ok();
}

void RaftNode::MarkHardStateDirty() { hard_state_dirty_ = true; }

void RaftNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  // Uniform in [election_tick, 2 * election_tick). Without randomization,
  // candidates time out together, split the vote, and repeat -- a cluster that
  // never elects anyone while looking perfectly healthy.
  std::uniform_int_distribution<uint32_t> dist(config_.election_tick,
                                               2 * config_.election_tick - 1);
  randomized_election_timeout_ = dist(rng_);
}

void RaftNode::BecomeFollower(uint64_t term, uint64_t leader) {
  if (term > term_) {
    term_ = term;
    vote_ = 0;  // a new term means the previous vote no longer applies
    MarkHardStateDirty();
  }
  role_ = Role::kFollower;
  leader_id_ = leader;
  ResetElectionTimer();
}

void RaftNode::BecomeCandidate() {
  ++term_;
  vote_ = config_.id;  // vote for self
  MarkHardStateDirty();
  role_ = Role::kCandidate;
  leader_id_ = 0;
  votes_.clear();
  votes_[config_.id] = true;
  ResetElectionTimer();
}

void RaftNode::BecomeLeader() {
  role_ = Role::kLeader;
  leader_id_ = config_.id;
  heartbeat_elapsed_ = 0;
  election_elapsed_ = 0;
  recent_active_.clear();
  recent_active_[config_.id] = true;

  const uint64_t last = log_.LastIndex();
  for (auto& [peer, progress] : progress_) {
    progress.next_index = last + 1;
    progress.match_index = peer == config_.id ? last : 0;
  }

  // A no-op entry from the new leader's own term.
  //
  // Without it, a leader whose log ends in entries from previous terms can
  // never commit them: the Figure 8 rule forbids committing an earlier-term
  // entry by replica count alone. Committing a current-term no-op carries all
  // the preceding entries with it indirectly, which is what makes the new
  // leader's state usable.
  proto::Entry noop;
  noop.set_term(term_);
  noop.set_index(last + 1);
  noop.set_type(proto::ENTRY_NOOP);
  const proto::Entry entries[] = {noop};
  (void)log_.Append(entries);
  progress_[config_.id].match_index = log_.LastIndex();
  progress_[config_.id].next_index = log_.LastIndex() + 1;

  LOG_DEBUG("node %llu became leader for term %llu", static_cast<unsigned long long>(config_.id),
            static_cast<unsigned long long>(term_));
  BroadcastAppend();
}

void RaftNode::Tick() {
  ++election_elapsed_;

  if (role_ == Role::kLeader) {
    ++heartbeat_elapsed_;
    if (heartbeat_elapsed_ >= config_.heartbeat_tick) {
      heartbeat_elapsed_ = 0;
      for (uint64_t peer : config_.peers) {
        if (peer != config_.id) {
          SendHeartbeat(peer);
        }
      }
    }

    // CheckQuorum. Figure 2 alone lets a partitioned leader stay leader
    // indefinitely -- it simply never hears the higher term that would demote
    // it. That is safe for the log, because it can never commit anything
    // without a quorum, but it leaves a node that believes it is leader and
    // would happily serve a stale read.
    //
    // So once per election timeout the leader asks whether it has actually
    // heard from a quorum, and steps down if not. Phase 4's ReadIndex still
    // confirms leadership per read; this shortens the window rather than
    // replacing that check.
    if (election_elapsed_ >= randomized_election_timeout_) {
      election_elapsed_ = 0;
      size_t active = 0;
      for (const auto& [peer, seen] : recent_active_) {
        if (seen) {
          ++active;
        }
      }
      recent_active_.clear();
      recent_active_[config_.id] = true;
      if (active < Quorum()) {
        LOG_DEBUG("node %llu stepping down: only %zu of %zu peers reachable",
                  static_cast<unsigned long long>(config_.id), active, config_.peers.size());
        BecomeFollower(term_, 0);
      }
    }
    return;
  }

  if (election_elapsed_ >= randomized_election_timeout_) {
    Campaign();
  }
}

void RaftNode::Campaign() {
  BecomeCandidate();

  // A single-node cluster wins immediately; there is no one to ask.
  if (config_.peers.size() == 1) {
    BecomeLeader();
    return;
  }

  const uint64_t last_index = log_.LastIndex();
  auto last_term = log_.TermAt(last_index);

  for (uint64_t peer : config_.peers) {
    if (peer == config_.id) {
      continue;
    }
    proto::Message m;
    m.set_type(proto::MSG_REQUEST_VOTE);
    m.set_from(config_.id);
    m.set_to(peer);
    m.set_term(term_);
    m.set_index(last_index);
    m.set_log_term(last_term.IsOk() ? *last_term : log_.SnapshotTerm());
    Send(std::move(m));
  }
}

void RaftNode::Step(const proto::Message& m) {
  // A message from a higher term always wins: step down and adopt it, then
  // process the message as a follower.
  if (m.term() > term_) {
    const uint64_t leader = (m.type() == proto::MSG_APPEND || m.type() == proto::MSG_HEARTBEAT ||
                             m.type() == proto::MSG_SNAPSHOT)
                                ? m.from()
                                : 0;
    BecomeFollower(m.term(), leader);
  }

  // A message from a lower term is stale. Reply so the sender learns the
  // current term and steps down, rather than letting it keep campaigning.
  if (m.term() != 0 && m.term() < term_) {
    if (m.type() == proto::MSG_REQUEST_VOTE) {
      proto::Message reply;
      reply.set_type(proto::MSG_REQUEST_VOTE_RESP);
      reply.set_from(config_.id);
      reply.set_to(m.from());
      reply.set_term(term_);
      reply.set_reject(true);
      Send(std::move(reply));
    } else if (m.type() == proto::MSG_APPEND || m.type() == proto::MSG_HEARTBEAT) {
      proto::Message reply;
      reply.set_type(m.type() == proto::MSG_APPEND ? proto::MSG_APPEND_RESP
                                                   : proto::MSG_HEARTBEAT_RESP);
      reply.set_from(config_.id);
      reply.set_to(m.from());
      reply.set_term(term_);
      reply.set_reject(true);
      Send(std::move(reply));
    }
    return;
  }

  if (role_ == Role::kLeader && m.term() == term_) {
    recent_active_[m.from()] = true;
  }

  switch (m.type()) {
    case proto::MSG_REQUEST_VOTE:
      HandleRequestVote(m);
      break;
    case proto::MSG_REQUEST_VOTE_RESP:
      HandleRequestVoteResponse(m);
      break;
    case proto::MSG_APPEND:
      HandleAppend(m);
      break;
    case proto::MSG_APPEND_RESP:
      HandleAppendResponse(m);
      break;
    case proto::MSG_HEARTBEAT:
      HandleHeartbeat(m);
      break;
    case proto::MSG_HEARTBEAT_RESP:
      // A heartbeat response confirms the follower is alive and at our term.
      // Replication progress is driven by AppendEntries responses.
      break;
    default:
      break;
  }
}

void RaftNode::HandleRequestVote(const proto::Message& m) {
  // Grant only if we have not already voted for someone else this term AND the
  // candidate's log is at least as up to date as ours. The second condition is
  // Leader Completeness: without it a node missing committed entries can win.
  const bool can_vote = vote_ == 0 || vote_ == m.from();
  const bool up_to_date = log_.IsUpToDate(m.log_term(), m.index());
  const bool grant = can_vote && up_to_date;

  if (grant) {
    vote_ = m.from();
    MarkHardStateDirty();
    // Only reset the timer when actually granting. Resetting on every request
    // would let a partitioned node that keeps campaigning hold off a healthy
    // election indefinitely.
    ResetElectionTimer();
  }

  proto::Message reply;
  reply.set_type(proto::MSG_REQUEST_VOTE_RESP);
  reply.set_from(config_.id);
  reply.set_to(m.from());
  reply.set_term(term_);
  reply.set_reject(!grant);
  Send(std::move(reply));
}

void RaftNode::HandleRequestVoteResponse(const proto::Message& m) {
  if (role_ != Role::kCandidate) {
    return;
  }
  votes_[m.from()] = !m.reject();

  size_t granted = 0;
  size_t rejected = 0;
  for (const auto& [peer, ok] : votes_) {
    (ok ? granted : rejected)++;
  }

  if (granted >= Quorum()) {
    BecomeLeader();
  } else if (rejected >= Quorum()) {
    // Cannot win this term. Step down and wait for the next timeout rather
    // than campaigning again immediately.
    BecomeFollower(term_, 0);
  }
}

void RaftNode::HandleAppend(const proto::Message& m) {
  leader_id_ = m.from();
  ResetElectionTimer();

  proto::Message reply;
  reply.set_type(proto::MSG_APPEND_RESP);
  reply.set_from(config_.id);
  reply.set_to(m.from());
  reply.set_term(term_);

  // The log must match at prev_index before anything is accepted.
  auto prev_term = log_.TermAt(m.index());
  const bool matches = prev_term.IsOk() && *prev_term == m.log_term();

  if (!matches) {
    reply.set_reject(true);

    // Fast backtracking. Rather than making the leader decrement next_index
    // once per round trip, report enough for it to skip the whole conflicting
    // term at once.
    if (m.index() > log_.LastIndex()) {
      // We are simply too short; the leader should back up to our end.
      reply.set_conflict_index(log_.LastIndex() + 1);
      reply.set_conflict_term(0);
    } else {
      auto conflicting = log_.TermAt(m.index());
      const uint64_t bad_term = conflicting.IsOk() ? *conflicting : 0;
      reply.set_conflict_term(bad_term);

      // First index we hold for that term.
      uint64_t first = m.index();
      while (first > log_.FirstIndex()) {
        auto t = log_.TermAt(first - 1);
        if (!t.IsOk() || *t != bad_term) {
          break;
        }
        --first;
      }
      reply.set_conflict_index(first);
    }
    Send(std::move(reply));
    return;
  }

  LOG_DEBUG("node %llu accept append from %llu term %llu prev=%llu/%llu count=%d",
            static_cast<unsigned long long>(config_.id), static_cast<unsigned long long>(m.from()),
            static_cast<unsigned long long>(m.term()), static_cast<unsigned long long>(m.index()),
            static_cast<unsigned long long>(m.log_term()), m.entries_size());

  const std::vector<proto::Entry> incoming(m.entries().begin(), m.entries().end());
  uint64_t first_changed = 0;
  const Status s = log_.TruncateAndAppend(m.index(), incoming, &first_changed);
  if (!s.IsOk()) {
    reply.set_reject(true);
    reply.set_conflict_index(log_.LastIndex() + 1);
    Send(std::move(reply));
    return;
  }

  // Anything rewritten must be persisted again, even if the log ended up the
  // same length. Without this a follower that truncated and refilled in one
  // step keeps the OLD entries on disk and resurrects them on restart.
  if (first_changed != 0) {
    persisted_index_ = std::min(persisted_index_, first_changed - 1);
  }

  if (m.commit() > commit_index_) {
    // Never advance past what we actually hold: the leader's commit index can
    // refer to entries it has not yet sent us.
    const uint64_t last_new = m.index() + incoming.size();
    commit_index_ = std::min(m.commit(), std::min(last_new, log_.LastIndex()));
    MarkHardStateDirty();
  }

  reply.set_reject(false);
  reply.set_index(log_.LastIndex());
  Send(std::move(reply));
}

void RaftNode::HandleHeartbeat(const proto::Message& m) {
  leader_id_ = m.from();
  ResetElectionTimer();

  if (m.commit() > commit_index_) {
    commit_index_ = std::min(m.commit(), log_.LastIndex());
    MarkHardStateDirty();
  }

  proto::Message reply;
  reply.set_type(proto::MSG_HEARTBEAT_RESP);
  reply.set_from(config_.id);
  reply.set_to(m.from());
  reply.set_term(term_);
  reply.set_index(log_.LastIndex());
  Send(std::move(reply));
}

void RaftNode::HandleAppendResponse(const proto::Message& m) {
  if (role_ != Role::kLeader) {
    return;
  }
  auto it = progress_.find(m.from());
  if (it == progress_.end()) {
    return;
  }
  PeerProgress& p = it->second;

  if (m.reject()) {
    // Skip the entire conflicting term in one step.
    uint64_t next = m.conflict_index();
    if (m.conflict_term() != 0) {
      // If we also hold that term, resume from just past our last entry in it.
      for (uint64_t i = log_.LastIndex(); i >= log_.FirstIndex(); --i) {
        auto t = log_.TermAt(i);
        if (t.IsOk() && *t == m.conflict_term()) {
          next = i + 1;
          break;
        }
        if (i == log_.FirstIndex()) {
          break;
        }
      }
    }
    p.next_index = std::max<uint64_t>(1, next);
    SendAppend(m.from());
    return;
  }

  // match_index only ever moves forward. A delayed response from an earlier
  // round can carry a lower index, and letting it pull match_index backwards
  // would un-commit entries.
  p.match_index = std::max(p.match_index, m.index());
  p.next_index = p.match_index + 1;
  MaybeAdvanceCommit();
}

void RaftNode::MaybeAdvanceCommit() {
  if (role_ != Role::kLeader) {
    return;
  }

  std::vector<uint64_t> matches;
  matches.reserve(progress_.size());
  for (const auto& [peer, p] : progress_) {
    matches.push_back(peer == config_.id ? log_.LastIndex() : p.match_index);
  }
  std::sort(matches.begin(), matches.end(), std::greater<>());
  const uint64_t candidate = matches[Quorum() - 1];

  if (candidate <= commit_index_) {
    return;
  }

  // Figure 8. A leader may only commit by replica count for entries from its
  // OWN term. An earlier-term entry replicated on a majority is NOT safe to
  // commit directly -- a later leader with a different history can still
  // overwrite it -- and it becomes committed only indirectly, once a
  // current-term entry above it commits.
  //
  // This is the rule that separates a Raft implementation that works from one
  // that loses acknowledged writes under a specific partition-and-recover
  // sequence, while passing every casual test.
  auto candidate_term = log_.TermAt(candidate);
  if (!candidate_term.IsOk() || *candidate_term != term_) {
    return;
  }

  commit_index_ = candidate;
  MarkHardStateDirty();
}

void RaftNode::SendAppend(uint64_t peer) {
  const PeerProgress& p = progress_[peer];
  const uint64_t prev_index = p.next_index - 1;
  auto prev_term = log_.TermAt(prev_index);

  proto::Message m;
  m.set_type(proto::MSG_APPEND);
  m.set_from(config_.id);
  m.set_to(peer);
  m.set_term(term_);
  m.set_index(prev_index);
  m.set_log_term(prev_term.IsOk() ? *prev_term : log_.SnapshotTerm());
  m.set_commit(commit_index_);

  const uint64_t hi = std::min(log_.LastIndex(), prev_index + config_.max_entries_per_append);
  for (const auto& entry : log_.Slice(p.next_index, hi)) {
    *m.add_entries() = entry;
  }
  Send(std::move(m));
}

void RaftNode::SendHeartbeat(uint64_t peer) {
  proto::Message m;
  m.set_type(proto::MSG_HEARTBEAT);
  m.set_from(config_.id);
  m.set_to(peer);
  m.set_term(term_);
  // Never tell a follower to commit past what we have sent it.
  m.set_commit(std::min(commit_index_, progress_[peer].match_index));
  Send(std::move(m));
}

void RaftNode::BroadcastAppend() {
  for (uint64_t peer : config_.peers) {
    if (peer != config_.id) {
      SendAppend(peer);
    }
  }
}

Status RaftNode::Propose(std::span<const std::byte> command) {
  if (role_ != Role::kLeader) {
    return Status::NotSupported("not the leader");
  }
  proto::Entry entry;
  entry.set_term(term_);
  entry.set_index(log_.LastIndex() + 1);
  entry.set_type(proto::ENTRY_NORMAL);
  entry.set_data(std::string(reinterpret_cast<const char*>(command.data()), command.size()));

  const proto::Entry entries[] = {entry};
  RAFTKV_RETURN_IF_ERROR(log_.Append(entries));
  progress_[config_.id].match_index = log_.LastIndex();
  progress_[config_.id].next_index = log_.LastIndex() + 1;

  BroadcastAppend();
  MaybeAdvanceCommit();
  return Status::Ok();
}

void RaftNode::Send(proto::Message message) { pending_messages_.push_back(std::move(message)); }

Ready RaftNode::ReadyToProcess() {
  Ready ready;

  // A follower that truncated a conflicting suffix now has a log SHORTER than
  // what it previously handed out to be persisted. Without this clamp, those
  // indices are never re-emitted, the durable log keeps the discarded entries,
  // and a restart resurrects them -- which shows up later as a Log Matching
  // violation on a node that looks perfectly healthy.
  persisted_index_ = std::min(persisted_index_, log_.LastIndex());

  if (hard_state_dirty_) {
    proto::HardState hs;
    hs.set_term(term_);
    hs.set_vote(vote_);
    hs.set_commit(commit_index_);
    if (hs.SerializeAsString() != last_hard_state_.SerializeAsString()) {
      ready.hard_state = hs;
    }
  }

  if (log_.LastIndex() > persisted_index_) {
    ready.entries_to_persist = log_.Slice(persisted_index_ + 1, log_.LastIndex());
  }

  ready.messages = pending_messages_;

  if (commit_index_ > applied_index_) {
    ready.committed_entries = log_.Slice(applied_index_ + 1, commit_index_);
  }
  return ready;
}

void RaftNode::Advance(const Ready& ready) {
  if (ready.hard_state.has_value()) {
    last_hard_state_ = *ready.hard_state;
    hard_state_dirty_ = false;
  }
  if (!ready.entries_to_persist.empty()) {
    persisted_index_ = ready.entries_to_persist.back().index();
  }
  if (!ready.committed_entries.empty()) {
    applied_index_ = ready.committed_entries.back().index();
  }
  pending_messages_.erase(
      pending_messages_.begin(),
      pending_messages_.begin() + static_cast<std::ptrdiff_t>(ready.messages.size()));
}

uint64_t RaftNode::MatchIndex(uint64_t peer) const {
  auto it = progress_.find(peer);
  return it == progress_.end() ? 0 : it->second.match_index;
}

uint64_t RaftNode::NextIndex(uint64_t peer) const {
  auto it = progress_.find(peer);
  return it == progress_.end() ? 0 : it->second.next_index;
}

}  // namespace raftkv::raft
