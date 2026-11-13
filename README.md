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

There are **no performance numbers yet**, because nothing has been benchmarked.
When numbers show up here they'll link to raw output in `results/`. No file, no
number. 295 tests, clang-tidy clean, green under ASan, UBSan and TSan, and the
Raft core also runs 2000 randomized fault scenarios (partitions, crashes,
dropped, delayed and duplicated messages) checking all five safety invariants
after every step. See [`VERIFIED_NUMBERS.md`](VERIFIED_NUMBERS.md) for what's
been audited and what can't be claimed yet.

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

## What it doesn't do yet

- **The real-process test is one kill, not a campaign.** No network partitions
  (needs Docker `tc`/`iptables`), no repeated runs, no linearizability checker.
- **Heavy concurrent writes starve heartbeats.** The driver holds its lock
  across each fsync, so under load a leader can lose leadership. Group commit
  is the fix.
- **Snapshots are sent whole**, in one message. Fine for megabytes, wrong for
  gigabytes.
- **Reads cost a quorum round trip.** No lease reads.
- **Sharding has one coordinator and no automatic balancing.** Keys are
  range-partitioned across Raft groups, and a shard can be split live (fence,
  copy, publish a new map, hand over, purge) while the rest of the key space
  keeps serving. Splits are resumable after a coordinator crash. But nothing
  decides *when* to split, there's no merge, and one split at a time per shard is
  assumed. There's no membership change, TLS or auth.
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

`src/shard/` holds the range shard map; `src/client/` has the sharded client, the directory and the split coordinator. `bench/` and `chaos/` are planned and still empty.
