#include "storage/raft_storage.h"

#include <algorithm>
#include <utility>

#include "lsm/coding.h"
#include "lsm/crc32c.h"

#include "common/log.h"

namespace raftkv::storage {
namespace {

constexpr size_t kRecordHeaderSize = 8;  // crc32 + length
constexpr const char* kHardStateName = "HARDSTATE";
constexpr const char* kHardStateTemp = "HARDSTATE.tmp";

std::string SegmentName(uint64_t first_index) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%020llu.log", static_cast<unsigned long long>(first_index));
  return buf;
}

bool ParseSegmentName(const std::filesystem::path& path, uint64_t* first_index) {
  if (path.extension() != ".log") {
    return false;
  }
  const std::string stem = path.stem().string();
  if (stem.empty() || stem.find_first_not_of("0123456789") != std::string::npos) {
    return false;
  }
  *first_index = std::stoull(stem);
  return true;
}

// Frames one payload with a CRC that covers the length field as well. Covering
// only the payload would let a corrupted length silently reframe every record
// after it, and each of those CRCs would still validate -- corruption that
// reads as valid data.
std::string FrameRecord(std::string_view payload) {
  std::string framed;
  framed.reserve(4 + payload.size());
  lsm::PutFixed32(&framed, static_cast<uint32_t>(payload.size()));
  framed.append(payload);

  std::string record;
  record.reserve(kRecordHeaderSize + payload.size());
  lsm::PutFixed32(&record, lsm::Crc32c(framed.data(), framed.size()));
  record.append(framed);
  return record;
}

// Reads every intact record from `contents`, stopping at the first damaged
// one and reporting how many bytes were good. A damaged tail is the expected
// result of a crash mid-append, not corruption.
Status ScanRecords(const std::string& contents, std::vector<std::string>* payloads,
                   uint64_t* good_bytes) {
  size_t offset = 0;
  while (offset < contents.size()) {
    if (contents.size() - offset < kRecordHeaderSize) {
      break;  // partial header
    }
    const uint32_t stored_crc = lsm::DecodeFixed32(contents.data() + offset);
    const uint32_t length = lsm::DecodeFixed32(contents.data() + offset + 4);
    if (contents.size() - offset - kRecordHeaderSize < length) {
      break;  // partial payload
    }
    const char* framed = contents.data() + offset + 4;
    if (lsm::Crc32c(framed, 4 + static_cast<size_t>(length)) != stored_crc) {
      break;  // torn or corrupt
    }
    payloads->emplace_back(contents, offset + kRecordHeaderSize, length);
    offset += kRecordHeaderSize + length;
  }
  *good_bytes = offset;
  return Status::Ok();
}

Result<std::string> ReadWholeFile(const std::filesystem::path& path) {
  auto opened = lsm::RandomAccessFile::Open(path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  auto file = opened.TakeValue();
  std::string contents;
  if (file->Size() > 0) {
    RAFTKV_RETURN_IF_ERROR(file->ReadExactly(0, static_cast<size_t>(file->Size()), &contents));
  }
  return contents;
}

}  // namespace

// ---------------------------------------------------------------------------
// HardStateStore
// ---------------------------------------------------------------------------

HardStateStore::HardStateStore(std::filesystem::path dir, proto::HardState current)
    : dir_(std::move(dir)), current_(std::move(current)) {}

Result<std::unique_ptr<HardStateStore>> HardStateStore::Open(const std::filesystem::path& dir) {
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    return Status::IoError("create " + dir.string() + ": " + ec.message());
  }

  proto::HardState state;
  const std::filesystem::path path = dir / kHardStateName;
  if (std::filesystem::exists(path, ec)) {
    auto contents = ReadWholeFile(path);
    if (!contents.IsOk()) {
      return contents.GetStatus();
    }
    // An empty file is a crash between create and write, not corruption.
    if (!contents->empty() && !state.ParseFromString(*contents)) {
      return Status::Corruption("hard state failed to parse: " + path.string());
    }
  }

  LOG_INFO("storage: hard state term=%llu vote=%llu commit=%llu",
           static_cast<unsigned long long>(state.term()),
           static_cast<unsigned long long>(state.vote()),
           static_cast<unsigned long long>(state.commit()));

  std::unique_ptr<HardStateStore> store(new HardStateStore(dir, std::move(state)));
  return store;
}

Status HardStateStore::Save(const proto::HardState& state) {
  const std::filesystem::path temp = dir_ / kHardStateTemp;
  const std::filesystem::path final_path = dir_ / kHardStateName;

  {
    auto created = lsm::WritableFile::Create(temp);
    if (!created.IsOk()) {
      return created.GetStatus();
    }
    auto file = created.TakeValue();
    RAFTKV_RETURN_IF_ERROR(file->Append(state.SerializeAsString()));
    // The temp file's CONTENTS must be durable before the rename publishes it,
    // or the rename can land pointing at an empty file.
    RAFTKV_RETURN_IF_ERROR(file->Sync());
    RAFTKV_RETURN_IF_ERROR(file->Close());
  }

  std::error_code ec;
  std::filesystem::rename(temp, final_path, ec);
  if (ec) {
    return Status::IoError("rename hard state: " + ec.message());
  }
  // And the rename itself is only durable once the directory entry is.
  RAFTKV_RETURN_IF_ERROR(lsm::SyncDirectory(dir_));

  current_ = state;
  return Status::Ok();
}

// ---------------------------------------------------------------------------
// RaftLogStore
// ---------------------------------------------------------------------------

RaftLogStore::RaftLogStore(LogStoreOptions options, std::vector<Segment> segments)
    : options_(std::move(options)), segments_(std::move(segments)) {}

Result<std::unique_ptr<RaftLogStore>> RaftLogStore::Open(LogStoreOptions options) {
  std::error_code ec;
  std::filesystem::create_directories(options.dir, ec);
  if (ec) {
    return Status::IoError("create " + options.dir.string() + ": " + ec.message());
  }

  std::vector<Segment> segments;
  for (const auto& entry : std::filesystem::directory_iterator(options.dir, ec)) {
    uint64_t first = 0;
    if (ParseSegmentName(entry.path(), &first)) {
      segments.push_back({first, entry.path()});
    }
  }
  if (ec) {
    return Status::IoError("scanning " + options.dir.string() + ": " + ec.message());
  }
  std::sort(segments.begin(), segments.end(),
            [](const Segment& a, const Segment& b) { return a.first_index < b.first_index; });

  std::unique_ptr<RaftLogStore> store(new RaftLogStore(std::move(options), std::move(segments)));

  // Recover: scan every segment, repairing a damaged tail in the last one.
  auto entries = store->ReadAll();
  if (!entries.IsOk()) {
    return entries.GetStatus();
  }
  if (!entries->empty()) {
    store->first_index_ = entries->front().index();
    store->last_index_ = entries->back().index();
  } else if (!store->segments_.empty()) {
    store->first_index_ = store->segments_.front().first_index;
    store->last_index_ = store->first_index_ - 1;
  }

  RAFTKV_RETURN_IF_ERROR(store->OpenActiveSegment());

  LOG_INFO("storage: raft log %llu..%llu across %zu segment(s)",
           static_cast<unsigned long long>(store->first_index_),
           static_cast<unsigned long long>(store->last_index_), store->segments_.size());
  return store;
}

Status RaftLogStore::OpenActiveSegment() {
  if (segments_.empty()) {
    const uint64_t first = last_index_ + 1;
    const std::filesystem::path path = options_.dir / SegmentName(first);
    auto created = lsm::WritableFile::Create(path);
    if (!created.IsOk()) {
      return created.GetStatus();
    }
    active_ = created.TakeValue();
    segments_.push_back({first, path});
    return lsm::SyncDirectory(options_.dir);
  }

  auto opened = lsm::WritableFile::OpenForAppend(segments_.back().path);
  if (!opened.IsOk()) {
    return opened.GetStatus();
  }
  active_ = opened.TakeValue();
  return Status::Ok();
}

Status RaftLogStore::RollSegment(uint64_t next_index) {
  RAFTKV_RETURN_IF_ERROR(active_->Sync());
  RAFTKV_RETURN_IF_ERROR(active_->Close());

  const std::filesystem::path path = options_.dir / SegmentName(next_index);
  auto created = lsm::WritableFile::Create(path);
  if (!created.IsOk()) {
    return created.GetStatus();
  }
  active_ = created.TakeValue();
  segments_.push_back({next_index, path});
  return lsm::SyncDirectory(options_.dir);
}

Status RaftLogStore::Append(std::span<const proto::Entry> entries) {
  for (const auto& entry : entries) {
    if (last_index_ != 0 && entry.index() != last_index_ + 1) {
      return Status::InvalidArgument("append at index " + std::to_string(entry.index()) +
                                     " leaves a hole after " + std::to_string(last_index_));
    }
    if (active_->Offset() >= options_.segment_bytes) {
      RAFTKV_RETURN_IF_ERROR(RollSegment(entry.index()));
    }
    RAFTKV_RETURN_IF_ERROR(active_->Append(FrameRecord(entry.SerializeAsString())));
    if (last_index_ == 0 && first_index_ > entry.index()) {
      first_index_ = entry.index();
    }
    last_index_ = entry.index();
    ++unsynced_entries_;
  }
  return Status::Ok();
}

Status RaftLogStore::Sync() {
  RAFTKV_RETURN_IF_ERROR(active_->Sync());
  unsynced_entries_ = 0;
  return Status::Ok();
}

Result<std::vector<proto::Entry>> RaftLogStore::ReadAll() {
  std::vector<proto::Entry> out;

  for (size_t i = 0; i < segments_.size(); ++i) {
    auto contents = ReadWholeFile(segments_[i].path);
    if (!contents.IsOk()) {
      return contents.GetStatus();
    }

    std::vector<std::string> payloads;
    uint64_t good_bytes = 0;
    RAFTKV_RETURN_IF_ERROR(ScanRecords(*contents, &payloads, &good_bytes));

    // A damaged tail is only acceptable in the LAST segment: that is where a
    // crash mid-append leaves one. Damage in the middle of the log means a
    // segment we already moved past is corrupt, and silently truncating there
    // would discard entries that follow it in later segments.
    if (good_bytes != contents->size()) {
      if (i + 1 != segments_.size()) {
        return Status::Corruption("raft log: damage in non-final segment " +
                                  segments_[i].path.string());
      }
      std::error_code ec;
      std::filesystem::resize_file(segments_[i].path, good_bytes, ec);
      if (ec) {
        return Status::IoError("truncating damaged log tail: " + ec.message());
      }
      LOG_WARN("storage: truncated damaged tail of %s to %llu bytes",
               segments_[i].path.string().c_str(), static_cast<unsigned long long>(good_bytes));
    }

    for (const std::string& payload : payloads) {
      proto::Entry entry;
      if (!entry.ParseFromString(payload)) {
        return Status::Corruption("raft log: entry failed to parse");
      }
      out.push_back(std::move(entry));
    }
  }
  return out;
}

Status RaftLogStore::TruncateSuffix(uint64_t index) {
  if (index > last_index_) {
    return Status::Ok();
  }

  auto entries = ReadAll();
  if (!entries.IsOk()) {
    return entries.GetStatus();
  }

  // Rewrite from scratch. Slower than seeking to a byte offset, and chosen
  // deliberately: a follower repairing a divergent suffix is rare, and a
  // rewrite cannot leave a half-updated index behind the way an in-place
  // truncation across a segment boundary can.
  RAFTKV_RETURN_IF_ERROR(active_->Close());
  std::error_code ec;
  for (const auto& segment : segments_) {
    std::filesystem::remove(segment.path, ec);
  }
  segments_.clear();
  active_.reset();

  std::vector<proto::Entry> kept;
  for (auto& entry : *entries) {
    if (entry.index() < index) {
      kept.push_back(std::move(entry));
    }
  }

  last_index_ = kept.empty() ? first_index_ - 1 : kept.back().index();
  RAFTKV_RETURN_IF_ERROR(OpenActiveSegment());
  if (!kept.empty()) {
    last_index_ = kept.front().index() - 1;
    RAFTKV_RETURN_IF_ERROR(Append(kept));
  }
  return Sync();
}

Status RaftLogStore::CompactPrefix(uint64_t index) {
  // Only whole segments are removed, and only those entirely below `index`.
  // Removing part of a segment would mean a rewrite; the point of segmenting
  // is that compaction is an unlink.
  std::error_code ec;
  size_t removable = 0;
  while (removable + 1 < segments_.size() && segments_[removable + 1].first_index <= index + 1) {
    ++removable;
  }
  for (size_t i = 0; i < removable; ++i) {
    std::filesystem::remove(segments_[i].path, ec);
    if (ec) {
      return Status::IoError("removing compacted segment: " + ec.message());
    }
  }
  if (removable > 0) {
    segments_.erase(segments_.begin(), segments_.begin() + static_cast<std::ptrdiff_t>(removable));
    first_index_ = segments_.front().first_index;
    RAFTKV_RETURN_IF_ERROR(lsm::SyncDirectory(options_.dir));
  }
  return Status::Ok();
}

}  // namespace raftkv::storage
