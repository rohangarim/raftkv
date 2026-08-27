# raftkv

A sharded, fault-tolerant, linearizable key-value store written from scratch in
C++20. Raft consensus implemented from the paper — no etcd, no braft, no
consensus library. gRPC on the wire, a hand-written LSM-tree engine underneath,
and a Jepsen-style harness that partitions the network and kills leaders to see
whether any of it actually holds up.

## Where this is

Phases 0 through 1b are done: the build, the storage engine, and the replicated
state machine. Phase 2 — the Raft core — is in progress. There is no working
cluster yet, and **no performance numbers**, because nothing has been
benchmarked. When numbers appear here, each one will link to raw output in
`results/` produced by a committed script. If there's no file, there's no
number.

110 tests, green under ASan, UBSan, and TSan.

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

**Applying a command doesn't fsync.** The obvious design fsyncs the state
machine on every apply, which means two fsyncs per write — one for the Raft log,
one for storage. What apply actually needs is *atomicity* of the effect and the
applied index, not durability. Both go into the same write batch, so they land
in one WAL record behind one CRC: a crash mid-write discards both or neither,
and anything lost is replayable from Raft's own durable log. The rule this trades
for is *never compact the Raft log past what storage has durably persisted*.

**Commands carry a client id and sequence number.** When a leader dies
mid-request, the client retries, and without dedup that write gets applied
twice. So the state machine keeps a session table mapping each client to its
last sequence and result, persisted in the same atomic batch as the write
itself, and included in every snapshot. Skipping this is how a system passes
all its own tests and then fails a linearizability checker.

**Only current-term entries commit by counting replicas.** This is the Figure 8
case from the Raft paper, and it's the thing most from-scratch implementations
get wrong. A leader that commits an earlier-term entry just because a majority
stores it can lose that entry to a later leader with a different history. Such
entries commit only indirectly, once a current-term entry above them commits.

Every non-obvious choice is written down in [`DECISIONS.md`](DECISIONS.md) with
its alternative and what it costs. If something here looks strange, the reason
is probably there.

## Layout

```
proto/          wire format — the single source of truth
src/raft/       consensus core: deterministic, no threads, no networking
src/lsm/        storage engine: WAL, memtable, SSTables, compaction, snapshots
src/statemachine/  the adapter Raft sees, plus the client session table
src/transport/  gRPC adapters
src/shard/      consistent hash ring and placement
src/client/     routing, leader cache, retry, dedup
bench/          load generator and latency histograms
chaos/          nemesis, history recorder, linearizability checker
results/        raw output, committed
```

## What this doesn't do

Worth saying plainly, since a system's limits are more useful than its claims:

- **No TLS, no auth.** Every channel is insecure. Not deployable as-is.
- **No live resharding and no membership change.** The cluster config is fixed
  at startup; changing it means a restart.
- **Client sessions never expire.** Every client id ever seen is kept forever.
  Expiry has to be deterministic across replicas, so a wall-clock TTL is wrong;
  the right fix is expiry by log position, and it isn't built.
- **Storage uses full compaction, not leveled.** Write amplification grows with
  database size. Fine for a bounded benchmark, wrong for a large dataset.
- **Snapshots block writes** for the length of a full compaction.
- **Docker on one machine is not a real network.** The chaos harness can drop,
  delay, and partition packets between five containers on one host. It cannot
  reproduce a real WAN, and its fsync goes through a virtualization layer.

This list grows as things become true, and nothing comes off it just because
it's inconvenient.
