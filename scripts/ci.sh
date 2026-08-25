#!/usr/bin/env bash
# Build and test every configuration. This is what CI runs and what must be
# green before a phase is declared done.
#
#   scripts/ci.sh              # default + all three sanitizers
#   scripts/ci.sh default      # one preset
#
# Sanitizer presets rebuild the fetched dependencies with the sanitizer
# enabled. That is slow on a cold cache (gRPC is a large tree) and it is
# deliberate: see DECISIONS.md, "Sanitizers apply to dependencies too".
set -euo pipefail

cd "$(dirname "$0")/.."

# Homebrew LLVM carries clang-format/clang-tidy, which Apple's CLT does not.
if [ -d /opt/homebrew/opt/llvm/bin ]; then
  export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
fi

# bash 3.2 (macOS) is unhappy with empty arrays under `set -u`; use a string.
PRESETS="$*"
if [ -z "${PRESETS}" ]; then
  PRESETS="default asan ubsan tsan"
fi

# Sanitizer runtime options. halt_on_error keeps a green run honest: a report
# that does not fail the build is a report nobody reads.
export ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:abort_on_error=1:strict_string_checks=1"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1:abort_on_error=1"
export TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1"

fail=0
for preset in ${PRESETS}; do
  echo "=============================================================="
  echo "== preset: ${preset}"
  echo "=============================================================="
  cmake --preset "${preset}"
  cmake --build --preset "${preset}"
  if ! ctest --preset "${preset}"; then
    echo "!! preset ${preset} FAILED"
    fail=1
  fi
done

exit "${fail}"
