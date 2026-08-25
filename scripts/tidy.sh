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

files=$(git ls-files 'src/*.cc' 'test/*.cc' 'bench/*.cc')
if [ -z "${files}" ]; then
  echo "tidy: no source files yet"
  exit 0
fi
count=$(echo "${files}" | wc -l | tr -d ' ')

echo "${files}" | xargs clang-tidy -p build --quiet
echo "tidy: clean (${count} files)"
