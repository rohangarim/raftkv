# Verified numbers

An audit of resume claims against what the code can actually prove. Every number
below came from a command run on this machine, with raw output committed under
`results/`. Nothing is estimated, extrapolated, or carried over from a README.

Audit date: 2026-09-06 (UTC).

Two repositories are involved:

| Repo | Path | HEAD | Tree |
|---|---|---|---|
| embedded-lsm-kv | `~/lsmtree` | `ff585d5` | **dirty — 7 files** |
| raftkv | `~/raft` | `0fb08b6` | clean |

> **Reproducibility caveat.** `~/lsmtree` had 6 files already modified and
> uncommitted before this audit began (`block.h`, `memtable.h`, `skiplist.h`,
> `block.cpp`, `test_block.cpp`, +204 lines) — pre-existing work that is not
> mine. A 7th file, `src/sstable.cpp`, I modified deliberately (see
> *Instrumentation added*). Every lsmtree number therefore describes a working
> tree, not a commit. Re-running from `ff585d5` alone will not reproduce them
> exactly.

---

## Machine and build

| | |
|---|---|
| CPU | Apple M4, 10 physical / 10 logical cores |
| RAM | 16 GiB |
| Disk | Apple Fabric NVMe SSD, APFS |
| OS | macOS 26.3.1 (build 25D771280a), Darwin 25.3.0 arm64 |
| Compiler | Apple clang 17.0.0 (clang-1700.6.4.2) |
| CMake / Ninja | 4.4.2 / 1.13.2 |
| Build type | RelWithDebInfo (`-O2 -g -DNDEBUG`) |

Full capture: [`~/lsmtree/results/00_machine.txt`](../lsmtree/results/00_machine.txt)

> **Throughput was measured twice, and the first set was invalid.** My initial
> runs were taken at load average 8.4–9.8 on a 10-core box, because the Claude
> desktop app hosting this session was consuming ~300% CPU across three
> processes and a stray `yes` process held another ~45%. Those numbers are
> preserved in `results/10_*` and `results/11_*` but **must not be used**: the
> LSM runs background compaction threads, and starving them stalls foreground
> writes. The machine later became quiet (load ~2.6), and re-running produced
> figures **3.3× higher**. All throughput numbers reported below come from the
> quiet re-runs, `results/13_*` and `results/14_*`.
>
> This is the single biggest correction in this audit, and it reversed a
> verdict. Read-path counter metrics (read amplification, false-positive rate)
> are unaffected — they count events, not time.

---

## Results

| # | Resume claim | Status | Measured | Command | Raw output |
|---|---|---|---|---|---|
| A1 | "sustaining 180K writes/sec across read-heavy, write-heavy, and mixed YCSB-style workloads" | **WRONG as stated** (number is met and exceeded; "across read-heavy" is false) | Write-heavy 432,579 writes/sec · mixed 230,398 writes/sec · read-heavy 43,829 and 0 writes/sec | `./build-audit/lsm_bench --db /tmp/lsm_bench_db [--threads N]` | `lsmtree/results/13_ycsb_quiet_3runs.txt`, `14_ycsb_quiet_threads.txt` |
| A2a | "…cut point-lookup read amplification by 60%…" | **WRONG** | 0.0% at 100% hits · **49.4% at a 50/50 mix** · 99.2% at 100% misses. 60% not reproduced at any measured ratio | `./build-audit/lsm_bloom_ab --keys 500000 --hits 200000 --misses 1000000 --bloom-bits {10,0} --sync never` | `lsmtree/results/20_bloom_ab_3runs.txt` |
| A2b | "…at a 1% false-positive rate" | **WRONG** | **1.2344%** over 999,938 filter probes | same as A2a | `lsmtree/results/20_bloom_ab_3runs.txt` |
| A3 | "kills the process mid-write across 500 randomized runs with zero acknowledged writes lost" | **VERIFIED** | 500/500 rounds passed, 472 killed mid-operation, 1,321,421 acknowledged writes, **0 lost** | `./build-audit/lsm_crash_harness --rounds 500 --worker ./build-audit/lsm_crash_worker --verbose` | `lsmtree/results/30_crash_harness_500.txt` |
| B1 | "Raised write throughput from 2.1K to 14K ops/sec at p99 under 25ms and 1.2s leader failover" | **NOT MEASURABLE** | — | — | — |
| B2 | "cleared 300 randomized fault-injection runs under network partitions and leader crashes with zero linearizability violations" | **NOT MEASURABLE** | — | — | — |

---

## A1 — throughput

Conditions, identical across all runs: 500,000 keys, 500,000 operations per
workload, 100-byte values, **`sync=never`** (buffered writes, no fsync per
operation), 8 MiB block cache, **checksums on**, **compression off**, Zipfian
θ=0.99, RelWithDebInfo, quiet machine (load ~2.6). Three runs; median reported.

**Median ops/sec**

| Workload | Mix | 1 thread | 4 threads | 8 threads |
|---|---|---|---|---|
| load | 100% insert | **432,579** | 427,032 | 426,679 |
| A | 50% read / 50% update | **460,796** | 432,576 | 397,812 |
| B | 95% read / 5% update | 876,585 | 1,474,057 | 1,075,890 |
| C | 100% read | 1,153,804 | 2,997,329 | 1,533,383 |
| D | 95% read / 5% insert | 1,202,158 | 2,438,051 | 1,422,025 |
| E | 95% scan / 5% insert | 151,809 | 437,324 | 419,921 |
| F | 50% read / 50% read-modify-write | 390,940 | 373,915 | 293,247 |

Run-to-run spread was ≤11.4% everywhere except workload F at 8 threads
(**28.5%**, flagged). Single-thread spreads were 1.3–8.5%.

**Converting to writes/sec, which is what the claim actually asserts**

| Workload | Category | ops/sec (1 thread) | write fraction | **writes/sec** | ≥180K? |
|---|---|---|---|---|---|
| load | write-heavy | 432,579 | 100% | **432,579** | yes |
| A | mixed | 460,796 | 50% | **230,398** | yes |
| F | mixed (RMW) | 390,940 | 50% | **195,470** | yes |
| B | read-heavy | 876,585 | 5% | **43,829** | no |
| D | read-heavy | 1,202,158 | 5% | **60,108** | no |
| C | read-heavy | 1,153,804 | 0% | **0** | no |

### Verdict

**The number is not the problem — the scope is.** At 180K writes/sec the claim
is conservative for the workloads that actually write: the engine sustains
432,579 writes/sec single-threaded on a pure insert workload, roughly 2.4× the
claimed figure, and clears 180K on both mixed workloads too.

**But "across read-heavy … workloads" is false, and false by construction.** A
read-heavy workload does not perform many writes. Workload B is 95% reads: at
876,585 ops/sec it manages 43,829 writes/sec. Workload C is 100% reads and
performs *zero* writes/sec. There is no throughput at which a read-heavy
workload sustains 180K writes/sec, because the writes are not in it. The bullet
reads as though one number characterises all seven workloads; it cannot.

**Write throughput does not scale with threads.** `load` is flat at 427K–433K
from 1 to 8 threads, while pure-read workload C scales from 1.15M to 3.0M at 4
threads. The write path is bound by compaction and log append, not CPU. Any
bullet implying threaded write scaling would be wrong.

**One condition the bullet must carry: `sync=never`.** These are buffered
writes. The engine's own default is `kEveryWrite`, which on macOS issues
`F_FULLFSYNC` per write; a 500K-key load under that policy did not finish in
11 minutes before I killed it. Quoting 432K without naming the fsync policy
turns a throughput number into a durability claim the engine cannot support.

## A2 — Bloom filters

Read amplification is defined, as suggested, as **SSTable data blocks read per
point lookup, counted at the block-cache miss path**. This is exactly what
`DbStats::sstable_blocks_read` counts: the counter is incremented in
`src/sstable.cpp` only after the cache-hit early return, in the `pread` path.
No new counter was needed.

Conditions: 500,000 stored keys, 100-byte values, 8 MiB block cache, checksums
on, compression off, `sync=never` (load phase only — sync policy cannot affect
read-path counters). Bloom ON = `bloom_bits_per_key=10`; OFF = empty filter.
Three runs per arm.

| Lookup mix | Read amp, Bloom ON | Read amp, Bloom OFF | Reduction |
|---|---|---|---|
| 100% present keys | 0.85447 | 0.85462 | **0.0%** |
| 50% present / 50% absent | 0.43222 | 0.85354 | **49.4%** |
| 100% absent keys | 0.00695 | 0.85346 | **99.2%** |

Spread across runs was ≤0.2% on every cell.

**The reduction is entirely a function of the miss ratio, and the claim states
no ratio.** For a key that exists, the filter always passes — it is a true
positive — so it saves nothing and the measured reduction is 0.0%. For a key
that is absent it saves nearly everything. 60% is not reproduced at any of the
three ratios measured; it sits between the 50/50 and all-miss cases, implying a
workload of roughly 60% misses, which the bullet does not mention. A
Bloom-filter reduction quoted without a hit/miss ratio is unanchored.

**False-positive rate at 10 bits/key: 1.2344%**, measured over 1,000,000
lookups of absent keys producing 999,938 filter probes (998,595 rejections,
12,343 pass-throughs). Identical across all three runs — the key sequence is
seeded, so this is deterministic rather than merely stable. For an absent key
every probe the filter fails to reject is by definition a false positive, so
`fp = (tables_probed − bloom_rejections) / tables_probed`.

The claimed 1% is optimistic by about 23% relative. The theoretical rate for
10 bits/key at k=6 is ~0.82%; the implementation uses double hashing, which
costs some accuracy for speed.

---

## A3 — crash durability

**VERIFIED.** 500 rounds, all passed.

| | |
|---|---|
| Rounds run | 500 (harness default is 200; `--rounds 500` was passed) |
| Killed by signal mid-operation | **472** |
| Rounds where writes had been acknowledged before the kill | 482 |
| Rounds killed before any acknowledgement | 18 |
| Sync policies covered | `every` 167 · `interval` 167 · `never` 166 |
| Total acknowledged writes | 1,321,421 |
| Total recovered | 1,321,785 |
| **Rounds losing an acknowledged write** | **0** |

Recovered exceeds acknowledged because writes issued after the last
acknowledgement may still reach disk; the harness asserts `recovered ≥ acked`
and additionally that recovered keys form a contiguous prefix, so a torn record
is never resurrected.

> **Scope limit worth stating on a resume.** The harness kills with `SIGKILL`,
> which terminates the process but leaves the OS page cache intact. Under the
> `never` policy — a third of the rounds — data reaches the page cache without
> an `fsync`, and survives a process kill for that reason. This demonstrates
> **process-crash** durability, not power-loss durability. Claiming the latter
> would require a machine-level fault injector or a simulated block device.

---

## B1 and B2 — raftkv: NOT MEASURABLE

### Honest inventory

| Component | State |
|---|---|
| `src/raft/` | **Exists and works.** 5 files. Full Figure 2: elections, log replication, conflict truncation, fast backtracking, Figure 8 commit rule, no-op on election, CheckQuorum |
| `src/lsm/` | Exists, 19 files |
| `src/statemachine/` | Exists, 4 files |
| `src/transport/` | Exists, 3 files — one `Ping` RPC only. **No Raft RPCs on gRPC** |
| `src/shard/` | **Empty directory, 0 files tracked** |
| `src/server/` | **Empty directory, 0 files tracked** |
| `src/client/` | **Empty directory, 0 files tracked** |
| `bench/` | **Empty directory, 0 files tracked** |
| `chaos/` | **Empty directory, 0 files tracked** |
| `docker/` | **Empty directory, 0 files tracked** |
| `results/` | Before this audit: `.gitkeep` and a README. **No measurements had ever been taken** |

Grepping the repo for `histogram`, `hdrhistogram`, `porcupine`, `linearizab`,
`nemesis`, and `failover` returns matches only inside **comments** describing
future phases. No such code exists.

The project is at phase 2 of 10. The Raft core is deliberately offline — no
threads, no sockets, no clock — and phase 3, which would put it on gRPC and a
real disk, has not started.

### B1 — "2.1K to 14K ops/sec at p99 under 25ms and 1.2s leader failover"

**NOT MEASURABLE.** Requires:

| Requirement | Exists? |
|---|---|
| A multi-node cluster that can serve a client write | **No** — Raft RPCs are not on the wire; there is no server binary and no client library |
| A load generator | **No** — `bench/` is empty |
| Latency histograms (p99) | **No** |
| A failover timer | **No** |
| Baseline vs optimized flags in one binary | **No** — the batching/pipelining work these numbers would compare is phase 9 |

Every number in this bullet is unsupported by the repository. Not one of the
three figures — 2.1K, 14K, p99 25ms, 1.2s — corresponds to anything that has
been run.

**Shortest path to measurability:** phase 3 (gRPC transport + durable Raft log),
then a minimal server binary and client, then a closed-loop load generator with
an HdrHistogram and a kill-the-leader timer. That is the bulk of phases 3, 4,
7, and 8. Realistically the largest remaining chunk of the project.

### B2 — "300 randomized fault-injection runs … zero linearizability violations"

**NOT MEASURABLE.** Requires a history recorder, a nemesis, and a
linearizability checker. `chaos/` is empty; none of the three exist.

**Important distinction, because there is something real nearby that must not be
confused with it.** `test/raft_cluster_test.cc` does run **2,000 randomized
seeds × 300 steps** against a simulated network with partitions, crashes,
restarts, and dropped/delayed/reordered/duplicated messages, asserting all five
Raft safety invariants after every step. That is real, it passes, and it found
a genuine durability bug (seed 5). But it is **an invariant check on internal
Raft state, not a linearizability check on an external client history.** It
runs in one process with no client, no concurrent operations, and no history
log. Presenting it as a linearizability result would be false.

**Shortest path to measurability:** a history recorder logging
`{process, type: invoke|ok|fail|info, op, key, value, wall_clock_ns}` — with
timeouts recorded as `info`, never `fail` — plus a nemesis driving `tc`/
`iptables` in Docker, plus Porcupine on an exported JSON history. Requires
phases 3, 4, 7, and 10, and a working cluster first.

---

## What IS measurable in raftkv today

| Metric | Value | Command | Raw |
|---|---|---|---|
| Test count | **141 tests, 141 pass** | `ctest --preset default` | `results/40_raftkv_tests_and_tidy.txt` |
| ASan | **141/141 pass** | `ctest --preset asan` | `results/41_raftkv_sanitizers.txt` |
| UBSan | **141/141 pass** | `ctest --preset ubsan` | `results/41_raftkv_sanitizers.txt` |
| TSan | **141/141 pass, zero race reports** — but the 2000-seed soak takes 132.2 s and exceeds ctest's 120 s timeout, so `ctest --preset tsan` reports a failure | `./build/tsan/test/raft_cluster_test --gtest_filter='*Randomized*'` | `results/41_raftkv_sanitizers.txt` |
| clang-tidy | **FAILING — 3 errors** | `./scripts/tidy.sh` | `results/40_raftkv_tests_and_tidy.txt` |
| Randomized Raft fault-injection | **2,000 seeds × 300 steps, 0 invariant violations** | `./build/test/raft_cluster_test --gtest_filter='*Randomized*'` | included in the ctest output above |

Two corrections to claims I made earlier in this project, which this audit
exists to catch:

1. The README states the suite is "green under ASan, UBSan, and TSan" at 141
   tests. **That run was never completed** — it was killed partway. Re-run
   properly: ASan and UBSan are green; TSan passes but exceeds the ctest
   timeout, so the suite as configured is *not* green.
2. clang-tidy is **red**: `bugprone-branch-clone` at `src/raft/raft_node.cc:245`,
   a naming violation at `src/raft/raft_node.h:103`, and `concurrency-mt-unsafe`
   at `test/raft_cluster_test.cc:346`. All three arrived with the phase-2 Raft
   core and were not caught before commit.

No LSM throughput benchmark exists in raftkv. Its `src/lsm/` engine is a
separate, younger implementation from `~/lsmtree` and has never been
benchmarked.

---

## Superseded and third-party files in `results/`

`results/10_ycsb_default_3runs.txt` and `results/11_ycsb_threads_4_8.txt` are
**superseded**. They were measured at load average 8.4–9.8 and understate
throughput by roughly 3.3×. They are kept because deleting a measurement
because it was inconvenient is exactly the habit this document exists to
prevent — but no number in this report comes from them.

Two further files, `results/12_ycsb_idle_3runs.txt` and
`results/21_bloom_ab_reproduce.txt`, appeared in the working tree during this
audit. **I did not produce them** — they were written at 22:41–22:43 UTC from a
`build-pristine` worktree that I never created. They are not committed and no
number in this report is drawn from them, because the rules here require that
every figure come from a command I ran myself.

## Instrumentation added for this audit

Disclosed in full. Neither change alters engine behaviour on any path a
benchmark exercises.

1. **`~/lsmtree/src/sstable.cpp`** (9 lines) — `TableBuilder::Finish()` now
   writes an *empty* Bloom block when `bloom_bits_per_key <= 0`, which
   `BloomFilter::MayContain` already decodes as "no filter, answer true".
   Needed because `BloomFilter::Build` floors at 64 bits and 1 probe, so
   passing 0 would otherwise still produce a working, rejecting filter and
   there would be no honest "disabled" arm. **No effect for any positive
   bits/key**, confirmed by `lsm_tests`: 182/182 pass after the change.

2. **`~/lsmtree/tools/bloom_ab.cpp`** (new, ~200 lines) plus two lines in
   `CMakeLists.txt` registering the `lsm_bloom_ab` target. Reads existing
   counters only; writes nothing to the engine.

   One flaw in my own tool, found and fixed before any number was recorded: the
   first version generated absent keys with a distinct `GONE` prefix, which
   sorted outside every SSTable's key range. The per-file range check discarded
   them *before any Bloom filter was consulted*, reporting `tables_probed = 0`.
   Had that gone unnoticed it would have measured the range check and called it
   a false-positive rate. Fixed by storing even indices and querying odd ones,
   so absent keys sit inside the stored range and the filter is genuinely
   exercised.

---

## Proposed rewrites

Using only VERIFIED numbers, in the original terse style.

**embedded-lsm-kv**

> Built an LSM-tree key-value store in C++; sustains 433K buffered writes/sec
> single-threaded and 3.0M reads/sec at 4 threads on YCSB-style workloads
> (500K keys, 100B values, sync=never, Apple M4).

> Added per-SSTable Bloom filters at 10 bits/key, cutting point-lookup read
> amplification 49% on a 50/50 hit-miss workload and 99% on all-miss lookups,
> at a measured 1.23% false-positive rate over 1M probes.

> Verified crash durability with a fault-injection harness: 500 randomized
> rounds, 472 SIGKILLs landing mid-write across three fsync policies, 1.3M
> acknowledged writes, zero lost.

**raftkv** — no performance claim is defensible yet. What is true:

> Implementing Raft consensus from the paper in C++20 — elections, log
> replication, fast conflict backtracking, and the Figure 8 commit rule — as a
> deterministic core with no threads, sockets, or clock.

> Validated it against a simulated hostile network: 2,000 seeded runs of
> partitions, crashes, restarts, and dropped/reordered/duplicated messages,
> asserting all five Raft safety invariants after every step; zero violations.
> Found a log-truncation durability bug this way that survives a naive
> length-comparison persistence check.

Notes on what these deliberately avoid:

- No throughput, latency, or failover number for raftkv. None has been measured.
- "Zero violations" is scoped to **Raft safety invariants**, not
  linearizability. Say invariants, and be ready to explain the difference.
- The lsmtree write figure names its fsync policy. Dropping "sync=never" would
  make 433K a durability claim it cannot support — a 500K-key load under the
  engine's default fsync-per-write policy did not finish in 11 minutes.
- It also avoids "across ... workloads". Write throughput characterises write
  workloads; quoting it over read-heavy ones is what made the original bullet
  wrong.
- The Bloom bullet carries its hit/miss ratio. Without it the percentage means
  nothing.
