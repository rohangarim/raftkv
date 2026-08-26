#include "lsm/status.h"

namespace raftkv::lsm {
namespace {

std::string_view CodeName(Code code) {
  switch (code) {
    case Code::kOk: return "OK";
    case Code::kNotFound: return "NotFound";
    case Code::kCorruption: return "Corruption";
    case Code::kIoError: return "IoError";
    case Code::kInvalidArgument: return "InvalidArgument";
    case Code::kNotSupported: return "NotSupported";
  }
  return "Unknown";
}

}  // namespace

std::string Status::ToString() const {
  std::string out(CodeName(code_));
  if (!message_.empty()) {
    out += ": ";
    out += message_;
  }
  return out;
}

}  // namespace raftkv::lsm
