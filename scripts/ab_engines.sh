#!/usr/bin/env bash
# Alternating A/B of the two storage engines, raw output saved verbatim.
#
#   scripts/ab_engines.sh <engine_bench|raftkv_bench> <rounds> <output-file> [tool args...]
#
# Engines alternate within each round (builtin, lsmtree, builtin, lsmtree, ...)
# rather than running all of one and then all of the other. The machine this is
# measured on is shared and its load drifts; running one engine's whole set first
# would hand it whatever the load happened to be at that moment. Interleaving
# spreads that drift across both. (An earlier test-suite comparison made exactly
# that mistake and drew a wrong conclusion from it: DECISIONS.md.)
#
# Load average is recorded before every run so a bad round can be recognised as
# one instead of being averaged in.
set -uo pipefail
cd "$(dirname "$0")/.."

TOOL="${1:?usage: ab_engines.sh <engine_bench|raftkv_bench> <rounds> <output-file> [args...]}"
ROUNDS="${2:?rounds}"
OUT="${3:?output file}"
shift 3

BIN=""
for candidate in "build/bench/${TOOL}" "build/${TOOL}"; do
  [ -x "${candidate}" ] && BIN="${candidate}" && break
done
[ -n "${BIN}" ] || { echo "no built ${TOOL}; run cmake --build --preset default" >&2; exit 1; }

{
  echo "# A/B of storage engines: ${TOOL}"
  echo "# recorded: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "# git: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- src bench 2>/dev/null || echo ' + uncommitted changes')"
  echo "# machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null), $(sysctl -n hw.ncpu 2>/dev/null) cores, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB, $(uname -sr)"
  echo "# shared machine: other work was running; load is recorded per run"
  echo "# command: ${BIN} --engine <builtin|lsmtree> $*"
  echo "# rounds: ${ROUNDS}, engines alternate within each round"
  echo
} > "${OUT}"

for round in $(seq 1 "${ROUNDS}"); do
  for engine in builtin lsmtree; do
    load="$(uptime | sed 's/.*averages: //' | cut -d' ' -f1)"
    {
      echo "## round ${round}: ${engine}   (load before: ${load})"
      "${BIN}" --engine "${engine}" "$@" 2>&1
      echo
    } >> "${OUT}"
  done
done
echo "wrote ${OUT}"
