#include "server/raft_driver.h"

#include <chrono>
#include <utility>

#include "common/log.h"

namespace raftkv::server {
namespace {

std::span<const std::byte> AsBytes(const std::string& s) {
  return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

}  // namespace

RaftDriver::RaftDriver(DriverOptions options,
                       std::shared_ptr<statemachine::StateMachine> state_machine,
                       std::unique_ptr<storage::HardStateStore> hard_state,
                       std::unique_ptr<storage::RaftLogStore> log)
    : options_(std::move(options)),
      state_machine_(std::move(state_machine)),
      hard_state_(std::move(hard_state)),
      log_(std::move(log)),
      read_epoch_(std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())) {}

RaftDriver::~RaftDriver() { Stop(); }

Result<std::unique_ptr<RaftDriver>> RaftDriver::Open(
    DriverOptions options, std::shared_ptr<statemachine::StateMachine> state_machine) {
  for (uint64_t peer : options.raft_config.peers) {
    if (peer == options.raft_config.id) {
      continue;
    }
    if (options.peer_addresses.find(peer) == options.peer_addresses.end()) {
      return Status::InvalidArgument("no address given for peer " + std::to_string(peer));
    }
  }

  auto hs_opened = storage::HardStateStore::Open(options.storage_dir / "hardstate");
  if (!hs_opened.IsOk()) {
    return hs_opened.GetStatus();
  }

  storage::LogStoreOptions log_options;
  log_options.dir = options.storage_dir / "log";
  auto log_opened = storage::RaftLogStore::Open(log_options);
  if (!log_opened.IsOk()) {
    return log_opened.GetStatus();
  }

  std::unique_ptr<RaftDriver> driver(new RaftDriver(std::move(options), std::move(state_machine),
                                                    hs_opened.TakeValue(), log_opened.TakeValue()));
  RAFTKV_RETURN_IF_ERROR(driver->Recover());
  return driver;
}

Status RaftDriver::Recover() {
  node_ = std::make_unique<raft::RaftNode>(options_.raft_config);
  node_->RestoreHardState(hard_state_->Current());

  auto entries = log_->ReadAll();
  if (!entries.IsOk()) {
    return entries.GetStatus();
  }
  if (!entries->empty()) {
    RAFTKV_RETURN_IF_ERROR(node_->RestoreLog(*entries));
  }

  {
    const std::lock_guard<std::mutex> lock(stubs_mutex_);
    for (auto& [peer, address] : options_.peer_addresses) {
      stubs_[peer] =
          proto::Raft::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    }
  }

  LOG_INFO("driver %llu: recovered term=%llu vote=%llu commit=%llu log=%llu..%llu",
           static_cast<unsigned long long>(options_.raft_config.id),
           static_cast<unsigned long long>(node_->Term()),
           static_cast<unsigned long long>(node_->Vote()),
           static_cast<unsigned long long>(node_->CommitIndex()),
           static_cast<unsigned long long>(log_->FirstIndex()),
           static_cast<unsigned long long>(log_->LastIndex()));
  return Status::Ok();
}

void RaftDriver::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;  // already running
  }
  ticker_ = std::thread([this] { TickLoop(); });
}

void RaftDriver::Stop() {
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false) && ticker_.joinable()) {
    ticker_.join();
  }

  // Outbound RPCs were dispatched on detached threads (see DispatchOutbound);
  // wait for them to finish rather than destroying stubs_ underneath them.
  std::unique_lock<std::mutex> lock(inflight_mutex_);
  inflight_drained_.wait(lock, [this] { return inflight_count_ == 0; });
}

Status RaftDriver::DrainAndPersist(DrainedReady* out) {
  raft::Ready ready = node_->ReadyToProcess();
  if (ready.Empty()) {
    return Status::Ok();
  }

  // Order matters and is the entire point of this function: everything that
  // must survive a crash is written before Advance() lets the core consider
  // it handled, and before any of it is returned to a caller who might act on
  // it (answer an RPC, dispatch a message).
  if (ready.hard_state.has_value()) {
    RAFTKV_RETURN_IF_ERROR(hard_state_->Save(*ready.hard_state));
  }
  if (!ready.entries_to_persist.empty()) {
    RAFTKV_RETURN_IF_ERROR(log_->Append(ready.entries_to_persist));
    RAFTKV_RETURN_IF_ERROR(log_->Sync());
  }

  // Copy, do not move. Advance() below reads committed_entries.back().index() to
  // learn how far the core has handed out. A moved-from protobuf reads as index
  // 0, so moving here made the core believe it had applied nothing and re-emit
  // the entire committed log on every drain.
  for (const auto& entry : ready.committed_entries) {
    apply_queue_.push_back(entry);
  }
  out->outbound = ready.messages;
  out->read_states = ready.read_states;
  out->dropped_reads = ready.dropped_reads;
  node_->Advance(ready);
  return Status::Ok();
}

void RaftDriver::Finish(DrainedReady drained) {
  ApplyPending();
  ResolveReads(drained.read_states, drained.dropped_reads);
  DispatchOutbound(std::move(drained.outbound));
}

void RaftDriver::ApplyPending() {
  // One consumer at a time, so apply order is queue order. A second thread
  // arriving while another is applying simply waits here; the first thread's
  // loop picks up whatever the second queued before releasing the mutex.
  const std::lock_guard<std::mutex> apply_lock(apply_mutex_);
  while (true) {
    std::vector<proto::Entry> batch;
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      if (apply_queue_.empty()) {
        return;
      }
      batch.assign(std::make_move_iterator(apply_queue_.begin()),
                   std::make_move_iterator(apply_queue_.end()));
      apply_queue_.clear();
    }
    ApplyBatch(batch);
  }
}

uint64_t RaftDriver::RedeliveredEntries() const {
  return redelivered_.load(std::memory_order_relaxed);
}

void RaftDriver::ApplyBatch(const std::vector<proto::Entry>& entries) {
  for (const auto& entry : entries) {
    // Committed entries must arrive strictly increasing. A repeat or a step
    // backwards is a bug upstream; skipping it is safe (the state machine would
    // dedup it anyway) but it must never be silent.
    if (entry.index() <= last_batch_index_) {
      if (redelivered_.fetch_add(1, std::memory_order_relaxed) == 0) {
        LOG_ERROR("driver %llu: entry %llu delivered after %llu; the core is re-emitting history",
                  static_cast<unsigned long long>(options_.raft_config.id),
                  static_cast<unsigned long long>(entry.index()),
                  static_cast<unsigned long long>(last_batch_index_));
      }
      continue;
    }
    last_batch_index_ = entry.index();

    // Entries without a command (no-op, config) are skipped by the state
    // machine, which is why the driver keeps its own applied index: see
    // AppliedRaftIndex().
    const bool has_command = entry.type() == proto::ENTRY_NORMAL;
    statemachine::ApplyResult result;
    if (has_command) {
      result = state_machine_->Apply(entry.index(), AsBytes(entry.data()));
      if (!result.status.IsOk() && !result.deduplicated) {
        LOG_WARN("driver %llu: apply at index %llu failed: %s",
                 static_cast<unsigned long long>(options_.raft_config.id),
                 static_cast<unsigned long long>(entry.index()), result.status.ToString().c_str());
      }
    }

    const std::lock_guard<std::mutex> lock(waiters_mutex_);
    applied_raft_index_ = entry.index();
    auto waiter = write_waiters_.find(entry.index());
    if (waiter != write_waiters_.end() && !waiter->second.done) {
      if (has_command && waiter->second.term == entry.term()) {
        waiter->second.result = std::move(result);
      } else {
        // Our proposal sat at this index, but the entry that actually
        // committed here belongs to a different term: a new leader overwrote
        // it. It did not commit, so the caller may safely report a clean
        // failure rather than an unknown outcome.
        waiter->second.status = Status::Unavailable("proposal was overwritten by a new leader");
      }
      waiter->second.done = true;
    }
  }
  waiters_cv_.notify_all();
}

void RaftDriver::ResolveReads(const std::vector<raft::ReadState>& states,
                              const std::vector<std::string>& dropped) {
  if (states.empty() && dropped.empty()) {
    return;
  }
  const std::lock_guard<std::mutex> lock(waiters_mutex_);
  for (const auto& rs : states) {
    auto it = read_waiters_.find(rs.context);
    if (it != read_waiters_.end()) {
      it->second.index = rs.index;
      it->second.done = true;
    }
  }
  for (const auto& ctx : dropped) {
    auto it = read_waiters_.find(ctx);
    if (it != read_waiters_.end()) {
      it->second.dropped = true;
      it->second.done = true;
    }
  }
  waiters_cv_.notify_all();
}

uint64_t RaftDriver::AppliedRaftIndex() const {
  const std::lock_guard<std::mutex> lock(waiters_mutex_);
  return applied_raft_index_;
}

proto::Raft::Stub* RaftDriver::StubFor(uint64_t peer) {
  const std::lock_guard<std::mutex> lock(stubs_mutex_);
  auto it = stubs_.find(peer);
  return it == stubs_.end() ? nullptr : it->second.get();
}

void RaftDriver::UpdatePeerAddress(uint64_t peer, const std::string& address) {
  auto stub =
      proto::Raft::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  const std::lock_guard<std::mutex> lock(stubs_mutex_);
  options_.peer_addresses[peer] = address;
  stubs_[peer] = std::move(stub);
}

void RaftDriver::DispatchOutbound(std::vector<proto::Message> outbound) {
  for (auto& message : outbound) {
    {
      const std::lock_guard<std::mutex> lock(inflight_mutex_);
      ++inflight_count_;
    }
    // Detached rather than joined here: DispatchOutbound must not block the
    // tick loop on network I/O to a peer, which is exactly the situation that
    // would let one slow follower stall replication to every other. Stop()
    // is where these are waited for.
    std::thread([this, message = std::move(message)]() mutable {
      SendOne(message);
      const std::lock_guard<std::mutex> lock(inflight_mutex_);
      if (--inflight_count_ == 0) {
        inflight_drained_.notify_all();
      }
    }).detach();
  }
}

void RaftDriver::SendOne(const proto::Message& message) {
  proto::Raft::Stub* stub = StubFor(message.to());
  if (stub == nullptr) {
    return;
  }

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::milliseconds(options_.tick_interval_ms * 5));

  proto::Message response;
  grpc::Status status;
  switch (message.type()) {
    case proto::MSG_REQUEST_VOTE:
      status = stub->RequestVote(&ctx, message, &response);
      break;
    case proto::MSG_APPEND:
    case proto::MSG_HEARTBEAT:
      // Both ride the AppendEntries RPC: on the wire only Message's own type
      // field distinguishes them, and the receiving handler dispatches on
      // that field via RaftNode::Step, not on which RPC method was called.
      status = stub->AppendEntries(&ctx, message, &response);
      break;
    default:
      // Nothing the core currently emits as leader-initiated outbound traffic
      // falls here. If something does (a future message type), dropping it
      // is the safe failure: Raft is built to tolerate lost messages, and a
      // retry follows from the next tick.
      return;
  }

  if (!status.ok()) {
    return;  // lost message; the protocol is built to tolerate this
  }

  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    node_->Step(response);
    const Status s = DrainAndPersist(&drained);
    if (!s.IsOk()) {
      LOG_ERROR("driver %llu: persisting after response from %llu failed: %s",
                static_cast<unsigned long long>(options_.raft_config.id),
                static_cast<unsigned long long>(message.to()), s.ToString().c_str());
      return;
    }
  }
  Finish(std::move(drained));
}

void RaftDriver::TickLoop() {
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(options_.tick_interval_ms));
    if (!running_.load(std::memory_order_relaxed)) {
      return;
    }

    DrainedReady drained;
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      node_->Tick();
      const Status s = DrainAndPersist(&drained);
      if (!s.IsOk()) {
        LOG_ERROR("driver %llu: persisting after tick failed: %s",
                  static_cast<unsigned long long>(options_.raft_config.id), s.ToString().c_str());
        continue;
      }
    }
    Finish(std::move(drained));
  }
}

Status RaftDriver::Propose(std::span<const std::byte> command) {
  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    RAFTKV_RETURN_IF_ERROR(node_->Propose(command));
    RAFTKV_RETURN_IF_ERROR(DrainAndPersist(&drained));
  }
  Finish(std::move(drained));
  return Status::Ok();
}

Result<statemachine::ApplyResult> RaftDriver::ProposeAndWait(std::span<const std::byte> command,
                                                             std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  DrainedReady drained;
  uint64_t index = 0;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    uint64_t term = 0;
    RAFTKV_RETURN_IF_ERROR(node_->Propose(command, &index, &term));
    // Registered while still holding mutex_ and BEFORE the entry can be
    // replicated, so the apply of this index cannot slip past unobserved.
    {
      const std::lock_guard<std::mutex> waiters_lock(waiters_mutex_);
      WriteWaiter waiter;
      waiter.term = term;
      write_waiters_[index] = std::move(waiter);
    }
    const Status s = DrainAndPersist(&drained);
    if (!s.IsOk()) {
      const std::lock_guard<std::mutex> waiters_lock(waiters_mutex_);
      write_waiters_.erase(index);
      return s;
    }
  }
  Finish(std::move(drained));

  std::unique_lock<std::mutex> lock(waiters_mutex_);
  const bool finished =
      waiters_cv_.wait_until(lock, deadline, [&] { return write_waiters_.at(index).done; });
  WriteWaiter waiter = std::move(write_waiters_.at(index));
  write_waiters_.erase(index);
  if (!finished) {
    return Status::TimedOut("timed out waiting for commit; the command may still apply");
  }
  if (!waiter.status.IsOk()) {
    return waiter.status;
  }
  return std::move(waiter.result);
}

Result<std::optional<std::string>> RaftDriver::LinearizableGet(std::string_view key,
                                                               std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  // The epoch keeps a context unique across restarts of this driver, so a
  // delayed heartbeat ack from a previous life can never match a new read.
  const std::string context = "r" + read_epoch_ + "." + std::to_string(read_counter_.fetch_add(1));

  {
    const std::lock_guard<std::mutex> lock(waiters_mutex_);
    read_waiters_[context] = ReadWaiter{};
  }
  auto forget = [&] {
    const std::lock_guard<std::mutex> lock(waiters_mutex_);
    read_waiters_.erase(context);
  };

  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    const Status s = node_->ReadIndex(context);
    if (!s.IsOk()) {
      forget();
      return s;
    }
    const Status persisted = DrainAndPersist(&drained);
    if (!persisted.IsOk()) {
      forget();
      return persisted;
    }
  }
  Finish(std::move(drained));

  uint64_t read_index = 0;
  {
    std::unique_lock<std::mutex> lock(waiters_mutex_);
    // Two waits, one deadline: first for a quorum to confirm leadership, then
    // for the state machine to catch up to the read point.
    if (!waiters_cv_.wait_until(lock, deadline, [&] { return read_waiters_.at(context).done; })) {
      read_waiters_.erase(context);
      return Status::TimedOut("timed out confirming leadership for a read");
    }
    const ReadWaiter waiter = read_waiters_.at(context);
    read_waiters_.erase(context);
    if (waiter.dropped) {
      return Status::Unavailable("lost leadership before the read was confirmed");
    }
    read_index = waiter.index;

    if (!waiters_cv_.wait_until(lock, deadline,
                                [&] { return applied_raft_index_ >= read_index; })) {
      return Status::TimedOut("timed out waiting for the state machine to reach the read point");
    }
  }
  return state_machine_->Get(key);
}

std::string RaftDriver::AddressOf(uint64_t node_id) const {
  const std::lock_guard<std::mutex> lock(stubs_mutex_);
  auto it = options_.peer_addresses.find(node_id);
  return it == options_.peer_addresses.end() ? std::string() : it->second;
}

Status RaftDriver::HandleRequestVote(const proto::Message& request, proto::Message* response) {
  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    node_->Step(request);
    RAFTKV_RETURN_IF_ERROR(DrainAndPersist(&drained));
  }

  bool found = false;
  std::vector<proto::Message> rest;
  for (auto& m : drained.outbound) {
    if (!found && m.to() == request.from()) {
      *response = m;
      found = true;
    } else {
      rest.push_back(std::move(m));
    }
  }
  drained.outbound = std::move(rest);
  Finish(std::move(drained));

  if (!found) {
    // A message from a stale term produces no reply at all (Step() drops it
    // silently in that one case); the caller of this RPC handler must not
    // block forever waiting for one, so this is reported rather than hung.
    return Status::IoError("no response produced for RequestVote from " +
                           std::to_string(request.from()));
  }
  return Status::Ok();
}

Status RaftDriver::HandleAppendEntries(const proto::Message& request, proto::Message* response) {
  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    node_->Step(request);
    RAFTKV_RETURN_IF_ERROR(DrainAndPersist(&drained));
  }

  bool found = false;
  std::vector<proto::Message> rest;
  for (auto& m : drained.outbound) {
    if (!found && m.to() == request.from()) {
      *response = m;
      found = true;
    } else {
      rest.push_back(std::move(m));
    }
  }
  drained.outbound = std::move(rest);
  Finish(std::move(drained));

  if (!found) {
    return Status::IoError("no response produced for AppendEntries from " +
                           std::to_string(request.from()));
  }
  return Status::Ok();
}

raft::Role RaftDriver::CurrentRole() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return node_->CurrentRole();
}

uint64_t RaftDriver::Term() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return node_->Term();
}

uint64_t RaftDriver::CommitIndex() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return node_->CommitIndex();
}

uint64_t RaftDriver::LeaderHint() const {
  const std::lock_guard<std::mutex> lock(mutex_);
  return node_->LeaderId();
}

}  // namespace raftkv::server
