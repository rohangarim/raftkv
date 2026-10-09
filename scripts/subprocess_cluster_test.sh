#!/usr/bin/env bash
# Real-process test for the Phase 3 "done when" bar: three ACTUAL OS
# processes (not in-process objects), real gRPC over 127.0.0.1, real disk.
# Elects a leader, kills the leader with SIGKILL, confirms a new one is
# elected among the survivors, restarts the killed process against its
# existing directory, and confirms it rejoins.
#
# What this exercises that an in-process test cannot: a kernel-delivered
# SIGKILL landing on a process at an arbitrary instant, mid-fsync included.
# Writes go in through the client CLI over real sockets, so "acknowledged"
# means exactly what it means to an application: the CLI exited 0.
#
# What it still does not do: many randomized runs, partitions (that needs
# tc/iptables in Docker, Phase 7), or any linearizability checking (Phase 10).
# One kill of one leader, with a fixed workload, is a smoke test of the real
# durability path, not a proof.
set -uo pipefail

cd "$(dirname "$0")/.."
BIN="build/src/server/raftkv_node"
CLI="build/src/client/raftkv_cli"
if [ ! -x "$BIN" ] || [ ! -x "$CLI" ]; then
  echo "FAIL: $BIN or $CLI not built; run cmake --build --preset default" >&2
  exit 1
fi

WORK=$(mktemp -d)
trap 'kill $PID1 $PID2 $PID3 2>/dev/null; wait 2>/dev/null; [ -n "${KEEP_WORK:-}" ] || rm -rf "$WORK"' EXIT

PEERS="1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003"
ADDRS="127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003"

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

# Writes through a real client. A write counts as acknowledged only if the CLI
# exits 0, i.e. the cluster told the client it committed.
ACKED=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  if "$CLI" --addrs "$ADDRS" --timeout-ms 8000 put "durable-$i" "value-$i" 2>"$WORK/cli.err"; then
    ACKED=$((ACKED + 1))
  else
    echo "FAIL: write durable-$i was not acknowledged: $(cat "$WORK/cli.err")"
    exit 1
  fi
done
echo "PASS: $ACKED writes acknowledged through the client"

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

# Every acknowledged write must be readable from the survivors, via a
# linearizable read that has to wait out the election first.
LOST=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  GOT=$("$CLI" --addrs "$ADDRS" --timeout-ms 10000 get "durable-$i" 2>"$WORK/cli.err")
  RC=$?
  if [ "$RC" -ne 0 ] || [ "$GOT" != "value-$i" ]; then
    echo "FAIL: durable-$i after the kill: rc=$RC got='$GOT' $(cat "$WORK/cli.err")"
    LOST=$((LOST + 1))
  fi
done
if [ "$LOST" -ne 0 ]; then
  echo "FAIL: $LOST of $ACKED acknowledged writes missing after SIGKILL of the leader"
  exit 1
fi
echo "PASS: all $ACKED acknowledged writes readable after SIGKILL of the leader"

# The cluster must still accept writes with the old leader gone.
if ! "$CLI" --addrs "$ADDRS" --timeout-ms 10000 put "after-kill" "still-works" 2>"$WORK/cli.err"; then
  echo "FAIL: write after failover was not acknowledged: $(cat "$WORK/cli.err")"
  exit 1
fi
echo "PASS: cluster accepts writes after failover"

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

# The restarted node's recovery line reports how much log it read back from its
# own disk. 10 writes plus the leader's no-op means at least 11 entries; a node
# that lost its log would report far fewer.
RECOVERED_LAST=$(grep "recovered term" "$WORK/node$LEADER1.log" | tail -1 \
  | sed -n 's/.*log=1\.\.\([0-9]*\).*/\1/p')
if [ -z "$RECOVERED_LAST" ] || [ "$RECOVERED_LAST" -lt 11 ]; then
  echo "FAIL: restarted node recovered only up to log index '${RECOVERED_LAST:-none}' from disk (expected >= 11)"
  exit 1
fi
echo "PASS: restarted node read $RECOVERED_LAST log entries back from its own disk"

echo "ALL PASS"
exit 0
