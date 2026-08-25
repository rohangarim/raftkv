#!/usr/bin/env bash
# Acquire pinned dependency sources into .deps/.
#
# This exists instead of letting FetchContent do the download because
# FetchContent's git step is all-or-nothing: if one of gRPC's eleven
# submodules drops its connection, CMake deletes the entire source tree and
# starts the multi-gigabyte clone again. On a flaky link that never converges.
#
# Here every clone is shallow, every submodule is fetched individually, and a
# failure retries only that submodule. Already-present work is kept, so a
# second run resumes rather than restarting.
#
# Idempotent: safe to run any number of times. CMake invokes it automatically.
set -uo pipefail

cd "$(dirname "$0")/.."
DEPS="${RAFTKV_DEPS_SRC:-$PWD/.deps}"
mkdir -p "${DEPS}"

GRPC_TAG="v1.75.1"
GTEST_TAG="v1.17.0"
ATTEMPTS=5

# Submodules a C++ build actually consumes. gRPC's --recursive default also
# pulls bloaty, benchmark, googletest and opentelemetry-cpp, which we never
# compile; each one is another chance for the network to fail the build.
GRPC_SUBMODULES="
third_party/abseil-cpp
third_party/boringssl-with-bazel
third_party/cares/cares
third_party/envoy-api
third_party/googleapis
third_party/opencensus-proto
third_party/protobuf
third_party/protoc-gen-validate
third_party/re2
third_party/xds
third_party/zlib
"

log() { printf '[fetch_deps] %s\n' "$*"; }

# retry <description> <command...>
retry() {
  desc="$1"; shift
  i=1
  while [ "${i}" -le "${ATTEMPTS}" ]; do
    if "$@"; then
      return 0
    fi
    log "${desc}: attempt ${i}/${ATTEMPTS} failed"
    i=$((i + 1))
    sleep $((i * 3))
  done
  log "ERROR: ${desc} failed after ${ATTEMPTS} attempts"
  return 1
}

clone_shallow() {
  url="$1"; tag="$2"; dest="$3"
  if [ -d "${dest}/.git" ]; then
    log "$(basename "${dest}") already present"
    return 0
  fi
  rm -rf "${dest}"
  retry "clone $(basename "${dest}")" \
    git clone --depth 1 --branch "${tag}" --quiet "${url}" "${dest}"
}

clone_shallow https://github.com/grpc/grpc.git "${GRPC_TAG}" "${DEPS}/grpc-src" || exit 1
clone_shallow https://github.com/google/googletest.git "${GTEST_TAG}" "${DEPS}/googletest-src" || exit 1

cd "${DEPS}/grpc-src"
failed=""
for sm in ${GRPC_SUBMODULES}; do
  if [ -n "$(ls -A "${sm}" 2>/dev/null)" ]; then
    continue
  fi
  log "submodule ${sm}"
  # --depth 1 keeps each submodule to a single commit; on a 1.3 GB recursive
  # tree that is the difference between minutes and tens of minutes.
  if ! retry "submodule ${sm}" \
      git submodule update --init --depth 1 --recommend-shallow --quiet "${sm}"; then
    failed="${failed} ${sm}"
  fi
done

if [ -n "${failed}" ]; then
  log "ERROR: submodules failed:${failed}"
  log "re-run scripts/fetch_deps.sh; completed submodules are kept"
  exit 1
fi

# Do not trust the loop above to have noticed a failure: git's own internal
# submodule retry can report success paths that leave a directory empty.
# Verify what is actually on disk before claiming this worked.
empty=""
for sm in ${GRPC_SUBMODULES}; do
  if [ -z "$(ls -A "${sm}" 2>/dev/null)" ]; then
    empty="${empty} ${sm}"
  fi
done
if [ -n "${empty}" ]; then
  log "ERROR: submodules present but empty:${empty}"
  exit 1
fi

log "ok: ${DEPS}"
