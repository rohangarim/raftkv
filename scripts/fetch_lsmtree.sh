#!/usr/bin/env bash
# Fetch the lsmtree storage engine at ONE pinned commit.
#
#   scripts/fetch_lsmtree.sh <full-sha> <destination-dir>
#
# Pinned by commit, not by branch: lsmtree is developed independently and its
# API moves, so tracking `main` would let a push over there break a build here
# with no change on this side. Moving the pin is a deliberate commit to this
# repository (RAFTKV_LSMTREE_SHA in cmake/Dependencies.cmake), made after the
# engine tests have been rerun against the new version.
#
# Shallow, retried, and published atomically: the destination either holds the
# pinned commit completely or does not exist, so an interrupted fetch can never
# leave a half-populated tree that the build would trust.
set -uo pipefail

SHA="${1:?usage: fetch_lsmtree.sh <full-sha> <destination-dir>}"
DEST="${2:?usage: fetch_lsmtree.sh <full-sha> <destination-dir>}"
URL="${RAFTKV_LSMTREE_URL:-https://github.com/rohangarim/embedded-lsm-kv.git}"
ATTEMPTS=5

log() { printf '[fetch_lsmtree] %s\n' "$*"; }

if [ -d "${DEST}/.git" ] && [ "$(git -C "${DEST}" rev-parse HEAD 2>/dev/null)" = "${SHA}" ]; then
  log "already at ${SHA:0:12}"
  exit 0
fi

PARTIAL="${DEST}.partial.$$"
rm -rf "${PARTIAL}"
mkdir -p "${PARTIAL}"
git init -q "${PARTIAL}"
git -C "${PARTIAL}" remote add origin "${URL}"

i=1
while [ "${i}" -le "${ATTEMPTS}" ]; do
  # Fetching a bare commit id works on GitHub for any reachable commit.
  if git -C "${PARTIAL}" fetch -q --depth 1 origin "${SHA}"; then
    break
  fi
  log "fetch attempt ${i}/${ATTEMPTS} failed"
  i=$((i + 1))
  if [ "${i}" -gt "${ATTEMPTS}" ]; then
    log "ERROR: could not fetch ${SHA:0:12} from ${URL}"
    rm -rf "${PARTIAL}"
    exit 1
  fi
  sleep $((i * 3))
done

git -C "${PARTIAL}" checkout -q FETCH_HEAD
GOT="$(git -C "${PARTIAL}" rev-parse HEAD)"
if [ "${GOT}" != "${SHA}" ]; then
  log "ERROR: fetched ${GOT} but ${SHA} was pinned"
  rm -rf "${PARTIAL}"
  exit 1
fi

rm -rf "${DEST}"
mv "${PARTIAL}" "${DEST}"
log "ok: ${SHA:0:12} -> ${DEST}"
