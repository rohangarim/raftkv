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
#
# LeakSanitizer does not exist on macOS/arm64, and asking for it is fatal
# rather than ignored: protoc is itself built with the sanitizer and runs
# during the build, so `detect_leaks=1` aborts code generation and the build
# fails long before any test runs. Enable leak detection only where it exists.
if [ "$(uname -s)" = "Linux" ]; then
  RAFTKV_LEAKS="detect_leaks=1"
else
  RAFTKV_LEAKS="detect_leaks=0"
fi
export ASAN_OPTIONS="${RAFTKV_LEAKS}:halt_on_error=1:abort_on_error=1:strict_string_checks=1"
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
    echo "!! preset ${preset} FAILED (builtin engine)"
    fail=1
  fi

  # The whole suite again on the other storage engine. RAFTKV_ENGINE makes every
  # test that brings up a state machine -- clusters, clients, shards, transactions,
  # snapshots -- use lsmtree without editing any of them, so a pass here is a
  # system-level statement, not just an engine-level one. Only if this build
  # actually contains lsmtree: DefaultKind() falls back (loudly) when it does not,
  # and rerunning the built-in engine under another name would prove nothing.
  if [ "${preset}" = "default" ]; then
    bindir="build"
  else
    bindir="build/${preset}"
  fi
  if "${bindir}/test/engine_conformance_test" --gtest_list_tests 2>/dev/null | grep -q '/lsmtree'; then
    echo "== preset: ${preset} (RAFTKV_ENGINE=lsmtree)"
    if ! RAFTKV_ENGINE=lsmtree ctest --preset "${preset}"; then
      echo "!! preset ${preset} FAILED (lsmtree engine)"
      fail=1
    fi
  else
    echo "== preset: ${preset}: lsmtree not built in; skipping the second-engine pass"
  fi
done

exit "${fail}"
