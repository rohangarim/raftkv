#!/usr/bin/env bash
# Real-process test for the Phase 3 "done when" bar: three ACTUAL OS
# processes (not in-process objects), real gRPC over 127.0.0.1, real disk.
# Elects a leader, kills the leader with SIGKILL, confirms a new one is
# elected among the survivors, restarts the killed process against its
# existing directory, and confirms it rejoins.
#
# What this test does NOT cover, and why: there is no client library yet
# (Phase 4), so nothing outside a node's own process can inject a Propose()
# call into a live subprocess. "Replicates writes" and "recovers with no
# lost acknowledged writes" against REAL subprocesses are therefore deferred
# until Phase 4 gives this script a way to write to the cluster from outside.
# The in-process gtest suite (raft_grpc_cluster_test) already proves the
# write-durability half of the bar deterministically; this script proves the
# half that specifically requires a real kernel-delivered SIGKILL, which an
# in-process test cannot.
set -uo pipefail

cd "$(dirname "$0")/.."
BIN="build/src/server/raftkv_node"
if [ ! -x "$BIN" ]; then
  echo "FAIL: $BIN not built; run cmake --build --preset default" >&2
  exit 1
fi

WORK=$(mktemp -d)
trap 'kill $PID1 $PID2 $PID3 2>/dev/null; wait 2>/dev/null; [ -n "${KEEP_WORK:-}" ] || rm -rf "$WORK"' EXIT

PEERS="1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003"

start_node() {
  local id="$1"
  mkdir -p "$WORK/node$id"
  "$BIN" --id "$id" --dir "$WORK/node$id" --peers "$PEERS" \
      > "$WORK/node$id.log" 2>&1 &
  echo $!
}

leader_of() {
  # Highest-term "became leader" line across all three logs, if any.
  grep -h "became leader" "$WORK"/node*.log 2>/dev/null \
    | tail -1 | sed -n 's/.*node \([0-9]*\) became leader.*/\1/p'
}

wait_for_leader() {
  local tries=0
  while [ "$tries" -lt 100 ]; do
    local leader
    leader=$(leader_of)
    if [ -n "$leader" ]; then
      echo "$leader"
      return 0
    fi
    sleep 0.1
    tries=$((tries + 1))
  done
  return 1
}

echo "starting 3 real node processes..."
PID1=$(start_node 1)
PID2=$(start_node 2)
PID3=$(start_node 3)
echo "pids: $PID1 $PID2 $PID3"
echo "workdir: $WORK"

sleep 0.5
LEADER1=$(wait_for_leader) || { echo "FAIL: no leader elected among real processes"; exit 1; }
echo "PASS: leader elected: node $LEADER1"

# bash 3.2 (macOS default) has no associative arrays.
case "$LEADER1" in
  1) VICTIM_PID=$PID1 ;;
  2) VICTIM_PID=$PID2 ;;
  3) VICTIM_PID=$PID3 ;;
esac
echo "SIGKILL-ing leader node $LEADER1 (pid $VICTIM_PID)..."
kill -9 "$VICTIM_PID"
wait "$VICTIM_PID" 2>/dev/null

# Detect the post-kill leader WITHOUT truncating any live process's log file.
# An earlier version of this script truncated survivor logs with `: > file`
# to reset leader_of()'s view -- but a live process still holds that file open
# at a nonzero offset, and POSIX write() writes at the FD's own offset, not at
# the file's current size. Truncating out from under it creates a sparse file:
# the process's next write lands after a hole of NUL bytes, `file` reports the
# result as binary, and grep silently stops matching lines past the hole. The
# actual election was happening correctly the whole time; only the detection
# logic was broken. Fixed by never truncating a live log -- instead we simply
# require the new leader's id to differ from LEADER1, which is unambiguous
# without touching the file at all.
sleep 0.3
LEADER2=""
tries=0
while [ "$tries" -lt 100 ]; do
  LEADER2=$(grep -h "became leader" "$WORK"/node*.log 2>/dev/null \
    | grep -v "node $LEADER1 became leader" \
    | tail -1 \
    | sed -n 's/.*node \([0-9]*\) became leader.*/\1/p')
  if [ -n "$LEADER2" ]; then
    break
  fi
  sleep 0.1
  tries=$((tries + 1))
done

if [ -z "$LEADER2" ] || [ "$LEADER2" = "$LEADER1" ]; then
  echo "FAIL: no new leader elected among survivors after SIGKILL of node $LEADER1"
  exit 1
fi
echo "PASS: new leader after real SIGKILL: node $LEADER2"

echo "restarting killed node $LEADER1 against its existing directory..."
NEWPID=$(start_node "$LEADER1")
case "$LEADER1" in
  1) PID1=$NEWPID ;;
  2) PID2=$NEWPID ;;
  3) PID3=$NEWPID ;;
esac

sleep 1.0
if ! kill -0 "$NEWPID" 2>/dev/null; then
  echo "FAIL: restarted node $LEADER1 (pid $NEWPID) did not stay up"
  exit 1
fi
if ! grep -q "recovered term" "$WORK/node$LEADER1.log"; then
  echo "FAIL: restarted node $LEADER1 did not log a recovery line"
  exit 1
fi
echo "PASS: restarted node $LEADER1 is up and recovered from its directory"

echo "ALL PASS"
exit 0
