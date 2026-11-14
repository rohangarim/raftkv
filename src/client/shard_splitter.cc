#include "client/shard_splitter.h"

#include <random>

#include "minilsm/coding.h"

#include "common/log.h"

namespace raftkv::client {
namespace {

uint32_t ReadU32(std::string_view v) {
  return static_cast<uint32_t>(static_cast<uint8_t>(v[0])) |
         static_cast<uint32_t>(static_cast<uint8_t>(v[1])) << 8 |
         static_cast<uint32_t>(static_cast<uint8_t>(v[2])) << 16 |
         static_cast<uint32_t>(static_cast<uint8_t>(v[3])) << 24;
}

uint64_t ReadU64(std::string_view v) {
  return static_cast<uint64_t>(ReadU32(v)) | static_cast<uint64_t>(ReadU32(v.substr(4))) << 32;
}

const char* const kPhases[] = {"started",  "fenced",    "copied", "published",
                               "released", "activated", "done"};
constexpr size_t kPhaseCount = sizeof(kPhases) / sizeof(kPhases[0]);

size_t PhaseIndex(const std::string& phase) {
  for (size_t i = 0; i < kPhaseCount; ++i) {
    if (phase == kPhases[i]) {
      return i;
    }
  }
  return kPhaseCount;
}

constexpr size_t kCopyChunk = 500;
constexpr uint32_t kPurgeChunk = 2000;

}  // namespace

struct ShardSplitter::Record {
  SplitRequest request;
  uint64_t epoch = 0;
  std::string src_lo;
  std::string src_hi;
  bool src_unbounded = true;
  std::vector<std::string> source_addresses;
  std::string old_map;  // serialized
  std::string new_map;  // serialized
  std::string phase = "started";

  std::string Serialize() const {
    std::string out;
    lsm::PutLengthPrefixed(&out, request.id);
    lsm::PutFixed32(&out, request.source_shard);
    lsm::PutLengthPrefixed(&out, request.split_key);
    lsm::PutFixed32(&out, request.target.shard_id);
    lsm::PutFixed32(&out, static_cast<uint32_t>(request.target.addresses.size()));
    for (const auto& a : request.target.addresses) {
      lsm::PutLengthPrefixed(&out, a);
    }
    lsm::PutFixed64(&out, epoch);
    lsm::PutLengthPrefixed(&out, src_lo);
    lsm::PutLengthPrefixed(&out, src_hi);
    out.push_back(src_unbounded ? 1 : 0);
    lsm::PutFixed32(&out, static_cast<uint32_t>(source_addresses.size()));
    for (const auto& a : source_addresses) {
      lsm::PutLengthPrefixed(&out, a);
    }
    lsm::PutLengthPrefixed(&out, old_map);
    lsm::PutLengthPrefixed(&out, new_map);
    lsm::PutLengthPrefixed(&out, phase);
    return out;
  }

  static Result<Record> Parse(std::string_view in) {
    auto bad = [] { return Status::Corruption("split record is malformed"); };
    Record r;
    auto str = [&](std::string* dst) {
      std::string_view s;
      if (!lsm::GetLengthPrefixed(&in, &s)) {
        return false;
      }
      *dst = std::string(s);
      return true;
    };
    auto u32 = [&](uint32_t* dst) {
      if (in.size() < 4) {
        return false;
      }
      *dst = ReadU32(in);
      in.remove_prefix(4);
      return true;
    };
    uint32_t n = 0;
    if (!str(&r.request.id) || !u32(&r.request.source_shard) || !str(&r.request.split_key) ||
        !u32(&r.request.target.shard_id) || !u32(&n)) {
      return bad();
    }
    for (uint32_t i = 0; i < n; ++i) {
      std::string a;
      if (!str(&a)) {
        return bad();
      }
      r.request.target.addresses.push_back(std::move(a));
    }
    if (in.size() < 8) {
      return bad();
    }
    r.epoch = ReadU64(in);
    in.remove_prefix(8);
    if (!str(&r.src_lo) || !str(&r.src_hi) || in.empty()) {
      return bad();
    }
    r.src_unbounded = in[0] != 0;
    in.remove_prefix(1);
    if (!u32(&n)) {
      return bad();
    }
    for (uint32_t i = 0; i < n; ++i) {
      std::string a;
      if (!str(&a)) {
        return bad();
      }
      r.source_addresses.push_back(std::move(a));
    }
    if (!str(&r.old_map) || !str(&r.new_map) || !str(&r.phase) || !in.empty()) {
      return bad();
    }
    r.request.target.start = r.request.split_key;
    return r;
  }
};

ShardSplitter::ShardSplitter(ShardDirectory* directory, ClientOptions base)
    : directory_(directory), base_(std::move(base)) {
  // A resumed coordinator must not reuse the sequence numbers of the one that
  // died: the groups remember the last one seen per client id and would reject
  // a lower number. So every instance takes a fresh identity.
  std::random_device rd;
  base_.client_id += "/split-" + std::to_string((static_cast<uint64_t>(rd()) << 32) | rd());
}

KvClient* ShardSplitter::ClientFor(const std::vector<std::string>& addresses,
                                   const std::string& tag) {
  auto it = clients_.find(tag);
  if (it == clients_.end()) {
    ClientOptions options = base_;
    options.addresses = addresses;
    options.client_id = base_.client_id + "/" + tag;
    it = clients_.emplace(tag, std::make_unique<KvClient>(std::move(options))).first;
  }
  return it->second.get();
}

Status ShardSplitter::Save(const Record& record) {
  return directory_->PutRecord("split/" + record.request.id, record.Serialize());
}

Status ShardSplitter::Split(const SplitRequest& request) {
  auto existing = directory_->GetRecord("split/" + request.id);
  if (!existing.IsOk()) {
    return existing.GetStatus();
  }
  const std::optional<std::string>& stored = *existing;
  if (stored.has_value()) {
    return Resume(request.id);
  }

  auto loaded = directory_->Load();
  if (!loaded.IsOk()) {
    return loaded.GetStatus();
  }
  const std::optional<shard::ShardMap>& old_map = *loaded;
  if (!old_map.has_value()) {
    return Status::InvalidArgument("the directory has no shard map to split");
  }
  const shard::ShardRange& source = old_map->Lookup(request.split_key);
  if (source.shard_id != request.source_shard) {
    return Status::InvalidArgument("split key does not belong to the named source shard");
  }
  shard::ShardRange target = request.target;
  auto next = old_map->Split(request.split_key, target);
  if (!next.IsOk()) {
    return next.GetStatus();
  }

  Record record;
  record.request = request;
  record.request.target.start = request.split_key;
  record.epoch = next->Epoch();
  record.src_lo = source.start;
  const size_t index = static_cast<size_t>(&source - old_map->Ranges().data());
  record.src_hi = old_map->EndOf(index);
  record.src_unbounded = record.src_hi.empty();
  record.source_addresses = source.addresses;
  record.old_map = old_map->Serialize();
  record.new_map = next->Serialize();
  record.phase = "started";

  // The record exists before anything is changed, so a crash at any later
  // point can be resumed from it.
  RAFTKV_RETURN_IF_ERROR(Save(record));
  return Run(&record);
}

Status ShardSplitter::Resume(const std::string& id) {
  auto stored = directory_->GetRecord("split/" + id);
  if (!stored.IsOk()) {
    return stored.GetStatus();
  }
  const std::optional<std::string>& bytes = *stored;
  if (!bytes.has_value()) {
    return Status::NotFound("no such split: " + id);
  }
  auto record = Record::Parse(bytes.value());
  if (!record.IsOk()) {
    return record.GetStatus();
  }
  Record r = record.TakeValue();
  return Run(&r);
}

Status ShardSplitter::Run(Record* record) {
  size_t phase = PhaseIndex(record->phase);
  if (phase >= kPhaseCount) {
    return Status::Corruption("split record has an unknown phase: " + record->phase);
  }
  const std::string& split_key = record->request.split_key;
  KvClient* source = ClientFor(record->source_addresses, "src");
  KvClient* target = ClientFor(record->request.target.addresses, "tgt");

  auto advance = [&](const char* next) -> Status {
    record->phase = next;
    phase = PhaseIndex(record->phase);
    RAFTKV_RETURN_IF_ERROR(Save(*record));
    LOG_INFO("split %s: %s", record->request.id.c_str(), next);
    return Status::Ok();
  };
  auto stop = [&](const char* step) { return stop_after && stop_after(step); };

  // 1. Fence the moving range on the source; make the target an empty owner.
  if (phase < PhaseIndex("fenced")) {
    KvClient::RangeSpec none;
    none.empty = true;
    none.epoch = record->epoch;
    RAFTKV_RETURN_IF_ERROR(target->SetOwnedRange(none));

    KvClient::RangeSpec fence;
    fence.lo = split_key;
    fence.hi = record->src_hi;
    fence.unbounded_hi = record->src_unbounded;
    fence.epoch = record->epoch;
    RAFTKV_RETURN_IF_ERROR(source->Fence(fence));
    RAFTKV_RETURN_IF_ERROR(advance("fenced"));
    if (stop("fenced")) return Status::Unavailable("stopped after fenced (test hook)");
  }

  // 2. Copy. The source is fenced, so what it holds for the range cannot change
  // under us; re-copying after a crash writes the same bytes again.
  if (phase < PhaseIndex("copied")) {
    std::string cursor = split_key;
    while (true) {
      auto page = source->Scan(cursor, record->src_unbounded ? "" : record->src_hi,
                               static_cast<uint32_t>(kCopyChunk));
      if (!page.IsOk()) {
        return page.GetStatus();
      }
      if (!page->pairs.empty()) {
        RAFTKV_RETURN_IF_ERROR(target->Import(page->pairs));
      }
      if (!page->more || page->pairs.empty()) {
        break;
      }
      cursor = page->pairs.back().first;
      cursor.push_back('\0');
    }
    RAFTKV_RETURN_IF_ERROR(advance("copied"));
    if (stop("copied")) return Status::Unavailable("stopped after copied (test hook)");
  }

  // 3. Publish the new layout. This is the point clients start routing to the
  // target, but it still refuses until it is activated below.
  if (phase < PhaseIndex("published")) {
    auto old_map = shard::ShardMap::Deserialize(record->old_map);
    auto new_map = shard::ShardMap::Deserialize(record->new_map);
    if (!old_map.IsOk() || !new_map.IsOk()) {
      return Status::Corruption("split record holds an unreadable shard map");
    }
    auto won = directory_->CompareAndPublish(std::optional<shard::ShardMap>(old_map.TakeValue()),
                                             *new_map);
    if (!won.IsOk()) {
      return won.GetStatus();
    }
    if (!*won) {
      // Either a previous attempt of ours already published it, or something
      // else changed the map. Only the former is acceptable.
      auto now = directory_->Load();
      if (!now.IsOk()) {
        return now.GetStatus();
      }
      const std::optional<shard::ShardMap>& current = *now;
      if (!current.has_value() || current->Serialize() != record->new_map) {
        return Status::Unavailable("the shard map changed during the split; it cannot continue");
      }
    }
    RAFTKV_RETURN_IF_ERROR(advance("published"));
    if (stop("published")) return Status::Unavailable("stopped after published (test hook)");
  }

  // 4. Release on the source, THEN 5. activate on the target. In this order no
  // client can read the moving range from the source after the target has
  // accepted a write to it.
  if (phase < PhaseIndex("released")) {
    KvClient::RangeSpec keep;
    keep.lo = record->src_lo;
    keep.hi = split_key;
    keep.epoch = record->epoch;
    RAFTKV_RETURN_IF_ERROR(source->SetOwnedRange(keep));
    RAFTKV_RETURN_IF_ERROR(advance("released"));
    if (stop("released")) return Status::Unavailable("stopped after released (test hook)");
  }
  if (phase < PhaseIndex("activated")) {
    KvClient::RangeSpec take;
    take.lo = split_key;
    take.hi = record->src_hi;
    take.unbounded_hi = record->src_unbounded;
    take.epoch = record->epoch;
    RAFTKV_RETURN_IF_ERROR(target->SetOwnedRange(take));
    RAFTKV_RETURN_IF_ERROR(advance("activated"));
    if (stop("activated")) return Status::Unavailable("stopped after activated (test hook)");
  }

  // 6. Reclaim the space. Safe to repeat, and refused by the group if it would
  // touch anything it still owns.
  if (phase < PhaseIndex("done")) {
    while (true) {
      KvClient::RangeSpec purge;
      purge.lo = split_key;
      purge.hi = record->src_hi;
      purge.unbounded_hi = record->src_unbounded;
      purge.epoch = record->epoch;
      purge.limit = kPurgeChunk;
      auto deleted = source->Purge(purge);
      if (!deleted.IsOk()) {
        return deleted.GetStatus();
      }
      if (*deleted == 0) {
        break;
      }
    }
    RAFTKV_RETURN_IF_ERROR(advance("done"));
  }
  return Status::Ok();
}

}  // namespace raftkv::client
