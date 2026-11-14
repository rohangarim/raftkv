#include "engine/engine.h"

#include <cstdlib>

#include "engine/builtin_engine.h"

#include "common/log.h"
#ifdef RAFTKV_HAVE_LSMTREE
#include "engine/lsmtree_engine.h"
#endif

namespace raftkv::engine {

std::string_view KindName(Kind kind) {
  switch (kind) {
    case Kind::kBuiltin:
      return "builtin";
    case Kind::kLsmTree:
      return "lsmtree";
  }
  return "unknown";
}

Result<Kind> ParseKind(std::string_view name) {
  if (name == "builtin" || name == "own") {
    return Kind::kBuiltin;
  }
  if (name == "lsmtree") {
    return Kind::kLsmTree;
  }
  return Status::InvalidArgument("unknown engine '" + std::string(name) +
                                 "' (expected 'builtin' or 'lsmtree')");
}

bool Available(Kind kind) {
#ifdef RAFTKV_HAVE_LSMTREE
  return kind == Kind::kBuiltin || kind == Kind::kLsmTree;
#else
  return kind == Kind::kBuiltin;
#endif
}

std::vector<Kind> AvailableKinds() {
  std::vector<Kind> kinds;
  for (Kind kind : {Kind::kBuiltin, Kind::kLsmTree}) {
    if (Available(kind)) {
      kinds.push_back(kind);
    }
  }
  return kinds;
}

Kind DefaultKind() {
  // getenv is flagged mt-unsafe because another thread could setenv
  // concurrently. Nothing here does; this is read when an engine is opened.
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char* requested = std::getenv("RAFTKV_ENGINE");
  if (requested == nullptr || *requested == '\0') {
    return Kind::kBuiltin;
  }
  auto parsed = ParseKind(requested);
  if (!parsed.IsOk()) {
    LOG_WARN("RAFTKV_ENGINE: %s; using the built-in engine", parsed.GetStatus().ToString().c_str());
    return Kind::kBuiltin;
  }
  if (!Available(*parsed)) {
    // A silent fallback here would let `RAFTKV_ENGINE=lsmtree ctest` report the
    // built-in engine's results as lsmtree's. Say so loudly instead.
    LOG_WARN(
        "RAFTKV_ENGINE=%s was requested but this build does not include it; using the "
        "built-in engine",
        requested);
    return Kind::kBuiltin;
  }
  return *parsed;
}

Result<std::unique_ptr<Engine>> Open(const Options& options) {
  if (!Available(options.kind)) {
    return Status::NotSupported("engine '" + std::string(KindName(options.kind)) +
                                "' is not part of this build");
  }
  switch (options.kind) {
    case Kind::kBuiltin:
      return BuiltinEngine::Open(options);
    case Kind::kLsmTree:
#ifdef RAFTKV_HAVE_LSMTREE
      return LsmTreeEngine::Open(options);
#else
      break;
#endif
  }
  return Status::NotSupported("unreachable engine kind");
}

}  // namespace raftkv::engine
