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
      log_(std::move(log)) {}

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

  out->committed_entries = ready.committed_entries;
  out->outbound = ready.messages;
  node_->Advance(ready);
  return Status::Ok();
}

void RaftDriver::ApplyCommitted(const std::vector<proto::Entry>& entries) {
  for (const auto& entry : entries) {
    // Noop and config entries carry no command. Skipping them means the state
    // machine's applied index can trail the Raft log's by the count of such
    // entries not yet followed by a real command -- harmless on its own, and
    // Phase 5's snapshot trigger is the place that must account for it (by
    // asking the state machine for its own applied index rather than
    // snapshotting at an arbitrary Raft log index). Not a Phase 3 concern:
    // nothing here calls TakeSnapshot yet.
    if (entry.type() != proto::ENTRY_NORMAL) {
      continue;
    }
    const statemachine::ApplyResult result =
        state_machine_->Apply(entry.index(), AsBytes(entry.data()));
    if (!result.status.IsOk() && !result.deduplicated) {
      LOG_WARN("driver %llu: apply at index %llu failed: %s",
               static_cast<unsigned long long>(options_.raft_config.id),
               static_cast<unsigned long long>(entry.index()), result.status.ToString().c_str());
    }
  }
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
  ApplyCommitted(drained.committed_entries);
  DispatchOutbound(std::move(drained.outbound));
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
    ApplyCommitted(drained.committed_entries);
    DispatchOutbound(std::move(drained.outbound));
  }
}

Status RaftDriver::Propose(std::span<const std::byte> command) {
  DrainedReady drained;
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    RAFTKV_RETURN_IF_ERROR(node_->Propose(command));
    RAFTKV_RETURN_IF_ERROR(DrainAndPersist(&drained));
  }
  ApplyCommitted(drained.committed_entries);
  DispatchOutbound(std::move(drained.outbound));
  return Status::Ok();
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
  ApplyCommitted(drained.committed_entries);
  DispatchOutbound(std::move(rest));

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
  ApplyCommitted(drained.committed_entries);
  DispatchOutbound(std::move(rest));

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
