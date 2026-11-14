# raftkv

A sharded, fault-tolerant, linearizable key-value store written from scratch in
C++20. Raft consensus implemented from the paper — no etcd, no braft, no
consensus library. gRPC on the wire, a hand-written LSM-tree engine underneath,
and a Jepsen-style harness that partitions the network and kills leaders to see
whether any of it actually holds up.

## Where this is

Three node processes talk gRPC, elect a leader, and replicate every write to
each follower's disk. A client library finds the leader, follows redirects,
retries safely, and does linearizable reads. Acknowledged writes survive the
leader being killed with a real `SIGKILL`, and a node that falls far behind
catches up from a snapshot instead of the log it no longer has.

Performance numbers are below, each backed by raw output in `results/`; no file,
no number. 324 tests, clang-tidy clean, and green under ASan, UBSan and TSan (one
timing-sensitive test failed once under load in an ASan run and passed on
rerun; the raw output is in `results/`). The Raft core also runs 2000 randomized fault scenarios (partitions, crashes,
dropped, delayed and duplicated messages) checking all five safety invariants
after every step. See [`VERIFIED_NUMBERS.md`](VERIFIED_NUMBERS.md) for what's
been audited and what can't be claimed yet.

## Performance

Measured on an Apple M4 laptop with `bench/raftkv_bench`: a 3-node Raft group
(two groups for the transaction rows) in **one process on loopback**, real gRPC,
real disk, 100-byte values, median of 3 repeats of 4 s
([`results/49`](results/49_raftkv_bench_no_commit_persist.txt)). That means no
real network between nodes and clients sharing cores with the servers, so these
describe the software on this machine, not a deployment.

| Operation | Threads | Rate | p50 latency |
|---|---:|---:|---:|
| Write, one key per request | 1 | 87 ops/s | 11 ms |
| Write, one key per request | 16 | 309 ops/s | 52 ms |
| Write, 100 keys per batch | 1 | 8.4K keys/s | 12 ms |
| Linearizable read | 1 / 4 | 6.0K / 11.3K ops/s | 0.17 / 0.35 ms |
| Scan, 100 rows | 1 | 29K rows/s | 3.4 ms |
| Transfer, both accounts on one shard | 1 | 80 txn/s | 12 ms |
| Transfer, accounts on two shards | 1 | 20 txn/s | 50 ms |

**Writes are bound by fsync.** On macOS the log syncs with `F_FULLFSYNC`, the
only call that really flushes the drive cache; it costs about 4 ms here, and a
replicated write pays it on the leader and again on a follower before it can be
acknowledged. A single writer can't beat that. Concurrent writers share syncs
(group commit): 16 threads went from 140 to 309 ops/s, while 1 thread stayed at
about 85 ([`results/52`](results/52_raftkv_bench_ab_group_commit.txt), median of
4 alternating runs against the previous build). It's still far from linear in
threads. Batching many keys into one entry remains the bigger lever: 100 keys in
one entry is roughly 100x the key rate.

**One change made this about twice as fast.** The commit index used to be saved
on every advance, a temp-file write plus a full fsync plus a rename plus a
directory fsync on every node's critical path, even though Raft doesn't need it to
be durable. Not saving it halved the write latency (A/B, alternating runs of the
old and new binaries, 4 rounds: one-key writes 38 to 72 ops/s, 100-key batches
3.6K to 7.7K keys/s;
[`results/50`](results/50_raftkv_bench_ab_commit_persist.txt)). Reads and scans
are unaffected, and the first baseline run
([`results/48`](results/48_raftkv_bench_baseline.txt)) was noisier than the later
ones, which is why that comparison uses alternating runs.

Run it yourself:

```bash
cmake --build --preset default --target raftkv_bench
build/bench/raftkv_bench --seconds 4 --repeat 3
```

## Build

```bash
cmake --preset default
cmake --build --preset default
ctest --preset default
```

You need CMake 3.24+, Ninja, and a C++20 compiler. Everything else — gRPC,
Protobuf, Abseil, GoogleTest — is pinned and fetched by the build. The first
configure downloads about 600 MB and takes a few minutes; after that it's
cached.

`scripts/ci.sh` runs all four presets including the three sanitizers. That's the
gate: nothing is "done" until it's green.

## How it fits together

A client hashes a key onto a consistent hash ring to find its shard, then talks
to that shard's Raft group directly — no coordinator hop. Each group is an
independent Raft instance with its own log, and one node process hosts several
groups. A write goes through consensus, and once committed it's applied to that
node's LSM engine, which is the replicated state machine.

The interesting part is the seam between Raft and storage. It's one narrow
interface (`src/statemachine/state_machine.h`) and the consensus core knows
nothing about how bytes reach a disk.

## Three decisions worth knowing about

**Applying a command doesn't fsync.** Fsyncing the state machine on every apply
means two fsyncs per write. Apply only needs the effect and the applied index to
land atomically (one write batch, one WAL record); anything lost is replayable
from Raft's own durable log. The catch: never compact the Raft log past what
storage has persisted.

**Commands carry a client id and sequence number.** If a leader dies
mid-request the client retries, and without dedup that write applies twice. A
session table, saved in the same atomic batch and included in every snapshot,
makes retries safe.

**Only current-term entries commit by counting replicas.** This is Figure 8 of
the Raft paper, the thing most from-scratch implementations get wrong. Earlier
entries commit only once a current-term entry above them does.

## How it's tested

The Raft core has no threads, sockets or clock, so a whole five-node cluster
runs inside one unit test and every run is a pure function of its seed. The
harness delays, reorders, drops and duplicates messages, partitions nodes, and
restarts them with only their persisted state. The five safety invariants are
checked after every step, so a bug is caught where it happens. A failing seed
replays exactly:

```bash
RAFTKV_SEED=5 ./build/test/raft_cluster_test --gtest_filter='*Randomized*'
```

That's how the nastiest bug turned up: a follower fixed its log, then reverted
after a restart. Persistence was decided by comparing log *lengths*, and a
truncate-and-refill leaves the length unchanged, so nothing was written and the
discarded entries came back.

Every non-obvious choice is in [`DECISIONS.md`](DECISIONS.md) with its
alternative and cost.

## Linearizability check

`test/linearizability_cluster_test` runs five concurrent clients doing puts,
gets and compare-and-swaps on five keys against a real 3-node gRPC cluster while
a thread kills and restarts nodes (leaders included), twice in a while two at
once so operations time out. It records every operation with start and end times
and checks the history with a Wing-Gong style search
(`test/linearizability.h`). An operation that timed out is treated as "may or
may not have happened". Fifteen seeds of about 1,100 operations each all passed
([`results/54`](results/54_raftkv_linearizability_campaign.txt)).

`test/process_chaos_test` runs the same workload against three real
`raftkv_node` processes. Nodes are killed with `SIGKILL`, and every node-to-node
link goes through a TCP relay the test can cut, which isolates a node (the
leader about half the time) without root or Docker. Twelve seeds of about 1,000
operations pass ([`results/55`](results/55_raftkv_process_chaos_campaign.txt)).
Its first runs found a real bug: after a follower's log diverged from a new
leader's, the driver never removed the stale entries from disk, and the node
then failed every tick for good. That's fixed (D-0056) and has a regression
test.

The checker is tested both ways: 13 unit tests of histories it must accept and
reject, and a mutation check. With reads that skip the leadership and
apply-point wait, the in-process campaign fails on 4 of 4 seeds and the process
campaign on 3 of 3, each printing the offending history. With client-session deduplication turned off it still passes, because
a retry after a lost reply is too rare in this setup for 1,100 operations to hit.

## What it doesn't do yet

- **No power-loss test, no real network.** Kills are `SIGKILL` and partitions
  are cut TCP relays on loopback, so a node never loses its OS page cache and
  there's no latency, reordering or asymmetric loss. Disk errors and clock
  faults aren't tested either.
- **Concurrent writers scale only about 2x.** Group commit shares the leader's
  sync, but followers still sync once per append and replication isn't pipelined
  beyond a per-peer cap.
- **Snapshots are sent whole**, in one message. Fine for megabytes, wrong for
  gigabytes.
- **Reads cost a quorum round trip.** No lease reads.
- **Sharding has one coordinator and only a crude balancer.** Keys are
  range-partitioned across Raft groups, and a shard can be split live (fence,
  copy, publish a new map, hand over, purge) while the rest of the key space
  keeps serving. Splits are resumable after a coordinator crash. A `ShardBalancer`
  counts keys per shard and splits the first one over a limit at its median, onto
  a spare group you provide. It runs when called (nothing schedules it), measures
  key count rather than bytes or load, reads a whole shard's keys to do it, and
  never merges. One split at a time per shard is assumed. There's no membership
  change, TLS or auth.
- **Transactions are optimistic and key-granular.** A write spanning shards
  commits with two-phase commit and is atomic, readers never see half of one,
  and a coordinator that dies is cleaned up after by whoever meets its locks.
  But reads are validated key by key, not by range, so a transaction can't
  guard against phantom inserts, and contended keys retry rather than queue.
- **Client sessions never expire**, and storage uses full compaction rather
  than leveled.

```
proto/   wire format           src/raft/        consensus core
src/lsm/ storage engine        src/statemachine/ what Raft applies to
src/storage/ durable log       src/server/      node process + driver
src/client/  leader-aware client   test/        unit, cluster and gRPC tests
```

`src/shard/` holds the range shard map; `src/client/` has the sharded client, the directory and the split coordinator. `bench/` has the benchmark tool; `chaos/` is planned and still empty.
