# raftkv

A sharded, fault-tolerant, linearizable key-value store, built from scratch in
C++20.

- Raft consensus implemented from the paper. No etcd, no braft, no consensus
  library.
- gRPC for all node-to-node and client-to-node communication.
- A hand-written LSM-tree storage engine as the replicated state machine.
- Keyspace sharded across replica groups by consistent hashing.
- Correctness checked by a Jepsen-style harness that partitions the network and
  kills leaders, then checks the recorded history for linearizability
  violations.

> **Status: Phase 0 of 10 complete.** The build system, the sanitizer gates, and
> one end-to-end gRPC round trip work. There is no Raft implementation yet, no
> storage engine, no sharding, and no measured performance. Every number in
> this file will arrive with a link to raw output in `results/`; there are no
> numbers in it today because nothing has been measured yet.

## Build

Requires CMake 3.24+, Ninja, a C++20 compiler, and Git. Everything else is
fetched and pinned by the build.

```bash
cmake --preset default
cmake --build --preset default
ctest --preset default
```

The first configure runs `scripts/fetch_deps.sh`, which shallow-clones gRPC and
GoogleTest into `.deps/` (about 600 MB), then builds gRPC, Protobuf, Abseil,
BoringSSL, and re2 from source. That is minutes, not seconds. Subsequent builds
reuse both.

`.deps/` is shared across build presets; object files are not. If a clone is
interrupted, re-run `scripts/fetch_deps.sh` directly — it resumes from what it
already has rather than starting over.

If you already have a matching gRPC install:

```bash
cmake --preset default -DRAFTKV_USE_SYSTEM_GRPC=ON
```

## Sanitizers

Three sanitizer presets, each with its own build directory. All three must be
green before a phase is considered done. TSan is not optional in this project;
it is a threaded replication system and TSan is the only tool that finds the
class of bug that shows up once a week in production.

```bash
scripts/ci.sh                # default + asan + ubsan + tsan
scripts/ci.sh tsan           # just one
```

Sanitizer presets rebuild the fetched dependencies with the sanitizer enabled.
That is slow and it is deliberate — see `DECISIONS.md` D-0004.

## Style

```bash
scripts/format.sh            # rewrite in place
scripts/format.sh --check    # CI mode
scripts/tidy.sh              # clang-tidy against build/compile_commands.json
```

On macOS, `clang-format` and `clang-tidy` come from Homebrew LLVM
(`brew install llvm`); the scripts add `/opt/homebrew/opt/llvm/bin` to `PATH`
themselves.

## Layout

```
proto/          wire format; the single source of truth
src/
  common/       logging and small shared utilities
  raft/         consensus core: deterministic, no networking, no threads
  transport/    gRPC client and server adapters
  storage/      raft log, WAL, persistent metadata (separate from the LSM WAL)
  statemachine/ LSM engine and the Raft state-machine adapter
  shard/        consistent hash ring, shard map, placement
  server/       node process: hosts N raft groups
  client/       client library: routing, leader cache, retry, dedup
bench/          load generator, latency histograms, failover timer
chaos/          nemesis scripts, history recorder, linearizability checker
test/
docker/
results/        raw benchmark and chaos output, committed
DECISIONS.md    one entry per real design decision
```

## Design

Every non-obvious choice is written down in [`DECISIONS.md`](DECISIONS.md) with
its alternative and its cost. If something in this repo looks strange, the
reason is probably there.

## Planned configuration

- 5 nodes, 5 Raft groups, replication factor 3. Each node leads roughly one
  group and follows roughly two.
- Consistent hash ring, 160 virtual nodes per physical node — a number to be
  justified against a measured distribution over 1M keys in Phase 6, not
  against folklore.
- Static cluster membership. Live membership change and live resharding are out
  of scope for v1.

## Limitations

Current and permanent, stated up front rather than discovered by a reader:

- **No TLS and no authentication.** All channels are insecure. Not deployable
  as-is.
- **No live resharding.** The shard map is fixed at cluster start.
- **No membership change.** Adding or removing a node means restarting the
  cluster with a new configuration.
- **Docker on a single machine is not a real network.** The chaos harness runs
  five containers on one host. It can drop, delay, and partition packets, but
  it cannot reproduce a real WAN's failure modes, and its `fsync` goes through
  a virtualization layer.
- **Benchmarks and chaos run in different environments** (native host and
  Docker Linux respectively) and each result is labelled with the one it came
  from. See `DECISIONS.md` D-0002.

Further limitations will be added here as they become true, not removed as they
become inconvenient.
