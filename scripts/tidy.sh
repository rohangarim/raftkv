#!/usr/bin/env bash
# Runs clang-tidy against the compilation database from the `default` preset.
# Generated protobuf/gRPC sources are excluded: they are not our code and they
# do not meet our warning bar.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -d /opt/homebrew/opt/llvm/bin ]; then
  export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
fi

DB="build/compile_commands.json"
if [ ! -f "${DB}" ]; then
  echo "no ${DB}; run: cmake --preset default && cmake --build --preset default" >&2
  exit 1
fi

files=$(git ls-files --cached --others --exclude-standard 'src/*.cc' 'test/*.cc' 'bench/*.cc')
if [ -z "${files}" ]; then
  echo "tidy: no source files yet"
  exit 0
fi
count=$(echo "${files}" | wc -l | tr -d ' ')

# Homebrew's clang-tidy does not know where Apple's libc++ headers live, so
# every <atomic>/<chrono> include fails to resolve and the resulting parse
# errors produce a flood of nonsense diagnostics. Hand it the SDK explicitly.
EXTRA=""
if [ "$(uname -s)" = "Darwin" ]; then
  SDK=$(xcrun --show-sdk-path 2>/dev/null || true)
  if [ -n "${SDK}" ]; then
    EXTRA="--extra-arg=-isysroot --extra-arg=${SDK}"
  fi
fi

# shellcheck disable=SC2086
echo "${files}" | xargs clang-tidy -p build --quiet ${EXTRA}
echo "tidy: clean (${count} files)"
