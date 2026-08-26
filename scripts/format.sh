#!/usr/bin/env bash
# scripts/format.sh          # rewrite files in place
# scripts/format.sh --check  # fail if anything is unformatted (CI mode)
#
# Written for bash 3.2 because that is what ships on macOS; no mapfile, no
# associative arrays.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -d /opt/homebrew/opt/llvm/bin ]; then
  export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
fi

# --others --exclude-standard includes files that are not yet tracked.
# Plain `git ls-files` sees only tracked files, which silently skipped every
# new source file until after its first commit -- exactly when formatting it
# matters most.
files=$(git ls-files --cached --others --exclude-standard '*.cc' '*.h' '*.cpp' '*.hpp')
if [ -z "${files}" ]; then
  echo "format: no source files yet"
  exit 0
fi
count=$(echo "${files}" | wc -l | tr -d ' ')

if [ "${1:-}" = "--check" ]; then
  echo "${files}" | xargs clang-format --dry-run --Werror
  echo "format: clean (${count} files)"
else
  echo "${files}" | xargs clang-format -i
  echo "format: rewrote ${count} files"
fi
