# Decisions

One entry per real decision. What we did, why, what the alternative was, what
it costs. Entries are append-only; if we reverse a decision we add a new entry
that supersedes the old one rather than editing history.

---

## D-0001: Build the LSM engine ourselves rather than adapting an existing one

**Phase:** 0 (affects 1)

**What:** The original plan assumed an existing LSM engine to wrap. There is
none on disk, so Phase 1 grows to include writing the engine: memtable,
SSTable format, WAL, compaction, and a snapshot mechanism.

**Why:** The state machine's contract with Raft is unusually strict --
deterministic apply, a consistent point-in-time snapshot, and atomic
persistence of `last_applied_index` alongside the command's effect. Retrofitting
those onto an engine that was not designed for them is normally harder than
writing an engine that has them from the first commit.

**Alternative:** Use RocksDB or LevelDB. Both give us atomic `WriteBatch` and
consistent snapshots for free and would cut Phase 1 to a day.

**Cost:** Phase 1 becomes the second-largest phase in the project. The engine
will be slower than RocksDB and we should say so in the README rather than
pretend otherwise. In exchange the atomicity story is ours to explain end to
end, which is the part that gets asked about.

---

## D-0002: Two measurement environments, labelled separately

**Phase:** 0 (affects 7, 8, 9, 10)

**What:** Benchmarks (Phases 8 and 9) run as native processes on the host Mac.
Chaos (Phases 7 and 10) runs in Docker Linux containers.

**Why:** `tc` and `iptables` only exist inside Linux containers, so partition
injection needs Docker. But on macOS, Docker is a Linux VM: the containers
share one virtualized NIC and one page cache, and `fsync` goes through a
virtio layer that does not have the host's durability or latency
characteristics. Latency numbers measured in there describe the VM, not the
machine.

**Alternative:** Run everything in Docker for a single consistent story. Or
run everything natively and give up real network partitions, faking them at
the transport layer instead.

**Cost:** Two environments to keep working, and the README has to carry two
hardware blocks instead of one. Transport-level fault injection also stays
useful (Phase 2's simulated network already does this deterministically), so
the Docker path is specifically for "the kernel dropped the packet", not for
"we returned an error from a mock".

---

## D-0003: gRPC and Protobuf pinned via FetchContent, with a system-package escape hatch

**Phase:** 0

**What:** `cmake/Dependencies.cmake` fetches gRPC `v1.75.1` by tag, which
vendors its own Protobuf and Abseil. `-DRAFTKV_USE_SYSTEM_GRPC=ON` switches to
`find_package` for developers who already have a matching install.

**Why:** A clean clone has to build with one command on both Ubuntu 22.04 and
macOS. Pinning by tag means a clone in two years builds the same bytes.
Critically, we do not mix a system Protobuf with a fetched gRPC: generated-code
ABI is not stable across Protobuf majors and that mismatch fails at link time
in a way that reads like a build-system bug for an hour before you find it.

**Alternative:** vcpkg. Genuinely better at dependency resolution, but it is a
second tool to install before `cmake -B build` works, which violates the
one-command constraint.

**Cost:** A cold configure clones and builds a large tree (gRPC + Abseil +
Protobuf + re2 + c-ares + zlib). That is minutes, not seconds, on first build.

---

## D-0004: Sanitizer presets rebuild dependencies with the sanitizer enabled

**Phase:** 0

**What:** The `asan`, `ubsan`, and `tsan` presets set `CMAKE_CXX_FLAGS`
globally, so the fetched dependencies are compiled with the sanitizer too. Each
sanitizer preset gets its own build directory under `build/`.

**Why:** This matters most for TSan. TSan reasons about happens-before edges by
instrumenting synchronization operations. gRPC does a great deal of its own
locking and lock-free signalling; if that code is uninstrumented, TSan cannot
see those edges and reports data races that are not races. A TSan run that
cries wolf is a TSan run the team learns to ignore, which is worse than not
running it.

**Alternative:** Attach sanitizer flags only to our own targets via an
interface library, leaving dependencies clean. Builds far faster and works
acceptably for ASan (mixing instrumented and uninstrumented objects is
supported; you just miss bugs in the uninstrumented parts). It does not work
for TSan without a large suppressions file, and a large suppressions file is a
place for real bugs to hide.

**Cost:** Four full builds of the dependency tree instead of one, and a cold
`scripts/ci.sh` is slow. Iteration uses the `default` preset; sanitizers are a
pre-merge gate, not an inner loop.

---

## D-0005: Warnings are attached to our targets only

**Phase:** 0

**What:** A `raftkv_warnings` INTERFACE library carries `-Wall -Wextra
-Wpedantic -Werror` plus `-Wshadow`, `-Wold-style-cast`, and friends. It is
linked into our libraries and tests. Fetched dependencies and generated
`.pb.cc` files are marked `SYSTEM` / compiled with `-w`.

**Why:** `-Werror` across a vendored tree means a dependency bump can break the
build for reasons that have nothing to do with our code. `-Werror` on our own
code means a shadowed variable in the Raft core cannot merge.

**Alternative:** Global `-Werror`. Honest, but it makes upgrading gRPC a
research project.

**Cost:** A bug that only manifests as a warning inside a dependency goes
unseen. Acceptable: that is what the sanitizers are for.

---

## D-0006: printf-shaped logging behind a relaxed atomic level check

**Phase:** 0

**What:** `src/common/log.h`. `LOG_DEBUG(...)` expands to a macro that tests
the level before evaluating any argument. No iostreams anywhere.

**Why:** The constraint is no logging on the hot path by default. A function
call with formatted arguments evaluates those arguments whether or not the
level is enabled -- so a `LOG_DEBUG("index=%llu", ComputeSomething())` costs
the computation even when logging is off. The macro form is the only way to
get the argument-evaluation guarantee, and there is a test asserting exactly
that (`test/log_test.cc`, `DisabledLevelDoesNotEvaluateArguments`).

`printf` format strings also get compile-time checking via
`__attribute__((format(printf, ...)))`, which catches the classic
"`%d` passed a `uint64_t`" bug at build time.

**Alternative:** `std::format` / `fmt`. Nicer syntax, type safe without
attributes. Rejected for now only because the level-check macro is what does
the real work, and `fmt` is another dependency; revisit if formatting
ergonomics start hurting.

**Cost:** Macros, and a fixed 1 KiB message buffer that truncates. Both are
visible and neither is load-bearing for correctness.

---

## D-0007: No TLS, no auth, in any phase

**Phase:** 0

**What:** All gRPC channels use insecure credentials.

**Why:** This is a consensus and storage project. TLS would add certificate
plumbing to every test fixture and Docker service and would teach us nothing
about Raft.

**Alternative:** mTLS between nodes, which is what a real deployment needs.

**Cost:** Not deployable as-is. This goes in the README limitations section
explicitly rather than being quietly omitted.

---

## D-0008: Tests bind ephemeral ports

**Phase:** 0

**What:** `NodeServer::Start("127.0.0.1:0", ...)` asks the OS for a port and
reports what it got.

**Why:** Hardcoded ports make a parallel `ctest` run fail intermittently, and
the failure looks like a distributed-systems bug rather than a port conflict.
Given that later phases are full of genuine timing-dependent failures, we
cannot afford a source of fake ones.

**Alternative:** A fixed port range per test binary.

**Cost:** Tests cannot assume a stable address, so fixtures have to thread the
bound address through. Minor.

---

## D-0009: A hand-written fetch script owns the download; FetchContent owns the build

**Phase:** 0. Supersedes the download half of D-0003; the pinning and the
no-mixed-Protobuf rule stand.

**What:** `scripts/fetch_deps.sh` clones gRPC and GoogleTest into `.deps/`
shallowly, initializes gRPC's submodules one at a time with `--depth 1`, and
retries each one individually up to five times. CMake runs it at configure time
via `execute_process` and then points FetchContent at the result with
`FETCHCONTENT_SOURCE_DIR_GRPC` / `_GOOGLETEST`. FetchContent still does
`add_subdirectory` and target wiring.

**Why:** Two separate problems, both observed rather than anticipated.

First, FetchContent's git step is all-or-nothing. gRPC's `--recursive` clone
pulls roughly 1.3 GB across a dozen submodules; when one connection was reset
mid-transfer, CMake deleted the entire source tree and restarted from zero.
This happened three times on this machine's link and never converged. The
per-submodule retry turns a fatal restart into a few seconds of lost work,
because everything already on disk is kept.

Second, restricting the submodule set to the eleven that a C++ build actually
compiles -- dropping `bloaty`, `benchmark`, `googletest`, and
`opentelemetry-cpp`, which we never reference -- took `.deps/` from 2.8 GB to
608 MB. Less to download is less to fail.

Separately, `SOURCE_DIR` is shared across presets while `BINARY_DIR` stays
inside each preset's build tree. Sources are identical for every preset;
objects are not, because the sanitizer presets compile with different flags and
sharing object files between them would be silently wrong.

**Alternative:** Retry `cmake --preset` in a loop and hope. Or vendor the
sources as a git submodule of this repo, which makes the clone the user's
problem instead of the build's.

**Cost:** A shell script is now load-bearing for the build, and the pinned
versions live in two places (`scripts/fetch_deps.sh` and
`cmake/Dependencies.cmake`) that can drift. The script is idempotent and
verifies every submodule directory is non-empty before reporting success, so
the failure mode is a loud error rather than a mysterious missing header.
`-DRAFTKV_FETCH_DEPS=OFF` disables it for environments that manage `.deps/`
themselves.

---

## D-0010: clang-tidy is given the macOS SDK path explicitly

**Phase:** 0

**What:** `scripts/tidy.sh` passes `--extra-arg=-isysroot $(xcrun
--show-sdk-path)` on Darwin.

**Why:** Homebrew LLVM's `clang-tidy` does not default to Apple's libc++
headers, so `#include <atomic>` failed to resolve. The interesting part is what
that produced: not one clear error, but a cascade of downstream nonsense,
including `cppcoreguidelines-pro-type-member-init` claiming that constructors
with complete member-initializer lists initialized nothing. Once the parse
succeeded, those diagnostics vanished on their own.

Worth remembering for later phases: when a static analyzer reports something
structurally impossible, suspect its parse before suspecting the code.

**Alternative:** Use Apple's bundled `clang-tidy`. It does not ship one.

**Cost:** `scripts/tidy.sh` has a platform branch in it.

---

## D-0011: Leak detection is enabled on Linux only

**Phase:** 0

**What:** `scripts/ci.sh` sets `detect_leaks=1` on Linux and `detect_leaks=0`
elsewhere.

**Why:** LeakSanitizer does not exist on macOS/arm64, and requesting it is
fatal rather than ignored: `AddressSanitizer: detect_leaks is not supported on
this platform` followed by `abort()`. The failure landed somewhere unexpected.
The ASan preset compiles `protoc` with ASan, and `protoc` *runs during the
build* to generate our `.pb.cc` files. So the sanitizer runtime aborted code
generation, and the ASan build died at step 2149 of 3142 with a message about
leak detection — nowhere near a test.

Two things worth carrying forward. First, when a preset builds its own
toolchain, sanitizer *runtime* options apply at build time, not just at test
time. Second, this is the first concrete instance of the macOS/Linux split from
D-0002: leak detection genuinely only runs in CI, so the local ASan run is
weaker than the CI one and we should not claim otherwise.

**Alternative:** Skip the ASan preset on macOS entirely. Rejected: ASan's
use-after-free and buffer-overflow checks are the ones that matter for the log
and WAL code, and those work fine here. Only the leak half is missing.

**Cost:** Leaks are caught in CI, not locally. A leak introduced on this Mac
survives until a Linux run notices it.

---

## D-0012: gtest discovery timeout raised to 120s

**Phase:** 0

**What:** `gtest_discover_tests(... DISCOVERY_TIMEOUT 120)`.

**Why:** A UBSan `ctest` run failed with "Process terminated due to timeout"
during test discovery. Measured directly afterwards, `--gtest_list_tests` on
that same binary takes 0.09 s, so this was a transient under load against
CMake's 5-second default, not a slow binary.

That distinction is the point. A discovery timeout aborts the entire `ctest`
run and reports as a hard failure, which in a project whose later phases are
full of real timing-dependent failures is exactly the wrong signal to have
firing at random. Later phases will have flaky-looking failures that are
genuine consensus bugs; the build system must not add fake ones to the pile.

**Alternative:** `DISCOVERY_MODE POST_BUILD`, which discovers at build time
instead. It makes the build slower and does not remove the timeout, just moves
it.

**Cost:** A genuinely hung test binary now takes two minutes to report instead
of five seconds.

---

## D-0013: We write the LSM engine ourselves; it lives in src/lsm/

**Phase:** 1a. Refines D-0001.

**What:** `src/lsm/` holds a complete storage engine: WAL, memtable, SSTable,
compaction, snapshots. `src/statemachine/` stays thin and will hold only the
Raft adapter (Phase 1b).

**Why:** The original layout put the engine behind `statemachine/` on the
assumption it was an external dependency being wrapped. Once we are writing it,
it is a peer component with its own tests, not an implementation detail of the
adapter.

**Cost:** One more top-level source directory than the original plan.

---

## D-0014: Apply does not fsync; the Raft log is the durability backstop

**Phase:** 1a. This is the load-bearing decision of the storage design.

**What:** `DB::Write` appends to the WAL but does not fsync by default.
`Options::sync_on_write` and `WriteOptions::sync` force it. `DB::SyncWal()`
exists as an explicit durability barrier.

**Why:** The state machine needs (effect, `last_applied_index`) to be ATOMIC,
not durable. Both live in the same `WriteBatch`, hence in the same WAL record,
hence behind a single CRC. A crash mid-append leaves a bad CRC, recovery
discards that record whole, and the effect and its index roll back together.
Whatever is lost is replayable from Raft's own durable log starting at the
recovered index.

Requiring an fsync here would mean two fsyncs per client write -- one for the
Raft log, one for the state machine -- roughly halving write throughput to
protect data that was already protected.

The invariant that replaces it, and that Phase 5 must respect: **never compact
the Raft log past what the LSM has durably persisted.** `SyncWal()` is called
before snapshot-driven Raft log truncation, not on the write path.

`test/lsm_db_test.cc::EffectAndAppliedIndexRollBackTogether` is the executable
form of this argument: it writes two complete batches, tears the third, and
asserts that both the value and the applied index recover to 2.

**Alternative:** fsync every apply. Simpler to explain, materially slower, and
protects nothing that Raft was not already protecting.

**Cost:** Correctness now depends on a cross-component invariant rather than on
a local guarantee. If Phase 5 truncates the Raft log without calling
`SyncWal()` first, data is genuinely lost and no test in `src/lsm/` will catch
it. That test has to live at the Raft layer.

---

## D-0015: macOS durability requires F_FULLFSYNC, not fsync

**Phase:** 1a

**What:** `WritableFile::Sync` issues `fcntl(F_FULLFSYNC)` on Apple platforms
and `fdatasync` elsewhere. Directory syncs use plain `fsync` on both.

**Why:** On macOS, `fsync()` pushes data to the drive but does not force the
drive's own write cache to persistent media. A benchmark that used `fsync`
would report durable-write numbers that are not durable, and the Phase 8
baseline (`--fsync_per_entry=true`) would be measuring the wrong thing.

**Alternative:** Use `fsync` and note the caveat. Rejected: the point of the
baseline is that it is honest.

**Cost:** `F_FULLFSYNC` is much slower than `fsync`, so macOS sync-mode numbers
will look worse than Linux `fdatasync` numbers for reasons that have nothing to
do with this code. The README has to say so when those numbers appear.

---

## D-0016: SSTables record their maximum sequence number in the footer

**Phase:** 1a

**What:** The footer carries `max_sequence` alongside the index offsets.
Recovery takes the maximum across the WAL and every table.

**Why:** Found by a failing test, not by design review. Once a memtable is
flushed, its WAL is deleted -- that is what bounds recovery time. So after a
flush the WAL no longer knows how far sequence numbers had advanced, and a
reopened database resumed at sequence 0.

The failure mode is worth remembering because it is silent. Reads run at
`last_sequence_` as their snapshot, so a snapshot of 0 hides every record ever
written. The database did not report corruption or an error; it reported that
it was empty.

**Alternative:** A separate MANIFEST file, which is what LevelDB does and which
also tracks which files belong to which level. That is the right answer once
levels get more complicated than "L0 and L1"; the footer field is the smaller
change that fully solves the problem we have.

**Cost:** Eight bytes per table, and the footer size changed, so any table
written before this commit is unreadable. Nothing has shipped, so no migration.

---

## D-0017: Write() releases the writer lock before triggering a flush

**Phase:** 1a

**What:** `DB::Write` scopes its `write_mutex_` guard and calls `MaybeFlush()`
after releasing it.

**Why:** `FlushMemTable` and `CompactRange` each acquire `write_mutex_`
themselves. Holding it across `MaybeFlush()` deadlocked on a non-recursive
mutex. The symptom was not an error but a hung write, which surfaced as a
ctest timeout after two minutes and looked at first like slow I/O.

The alternative fix -- making the mutex recursive -- was rejected. A recursive
mutex would have made this deadlock disappear while leaving the underlying
confusion about which function owns which lock intact, and that confusion gets
much more expensive once Phase 3 adds the Ready loop.

**Alternative:** Unlocked internal variants (`FlushMemTableLocked`) called from
inside the critical section. Slightly faster, and worth doing if flush latency
shows up in a profile.

**Cost:** A narrow window between releasing the lock and calling `MaybeFlush`
in which another writer can add to the memtable. Harmless: the flush trigger is
a threshold, not a hard limit.

---

## D-0018: Protobuf commands are safe because we never re-serialize them

**Phase:** 1b

**What:** `proto/kv.proto` defines `Command`, which is what Raft replicates.

**Why this needs saying:** Protobuf serialization is not canonical. The same
message can encode to different bytes across library versions and language
implementations, which normally disqualifies it from a replicated state
machine, where replicas must agree byte for byte.

It is safe here because of an invariant, not a property of protobuf: the leader
serializes a `Command` exactly once, Raft replicates those bytes verbatim, and
every replica parses the identical bytes. Parsing is deterministic. Determinism
would break the instant any code path parsed a command and re-serialized it
before applying, so `kv.proto` carries a comment saying not to add one.

**Alternative:** A hand-rolled canonical encoding, which removes the footgun
entirely at the cost of writing and testing our own wire format.

**Cost:** A latent trap for a future contributor. The mitigation is a comment
and this entry, which is weaker than a mechanism.

---

## D-0019: One-byte key namespaces, applied by the adapter

**Phase:** 1b

**What:** Every key stored in the engine carries a namespace tag: `'u'` user
data, `'m'` metadata, `'s'` sessions.

**Why:** Client keys are arbitrary bytes, so metadata cannot be hidden behind a
magic prefix that a client could also produce -- including `\0`-prefixed keys.
Because the tag is prepended by the adapter and never taken from client input,
no request can address the metadata or session namespaces.
`ClientKeysCannotReachInternalNamespaces` writes a client key literally named
`applied_index` and asserts the real applied index is untouched.

**Cost:** One byte per key, and the tags can never change without making every
existing database unreadable.

---

## D-0020: Effect, session entry, and applied index share one WriteBatch

**Phase:** 1b. The point of D-0014's machinery.

**What:** A single `Apply()` writes the user effect, the updated session entry,
and the new applied index into one `lsm::WriteBatch`.

**Why:** One batch is one WAL record behind one CRC, so a crash leaves all
three or none. Each weaker combination is a specific bug:

- effect without session entry -> the client's retry is applied a second time
- session entry without effect -> the retry returns success for a write that
  never happened, silently losing data
- either without the applied index -> Raft replays the entry and applies it
  again

**Alternative:** Three separate writes with a fsync between them. Correct and
roughly three times slower.

**Cost:** None material. This is what the storage engine was built for.

---

## D-0021: A failed CAS is a successful apply

**Phase:** 1b

**What:** `ApplyResult` carries `cas_mismatch` separately from `status`. A
comparison that fails returns `Status::Ok()` with `cas_mismatch` set.

**Why:** They mean different things to a client. A non-Ok status means "this
did not happen, retry is reasonable." A CAS mismatch means "this happened
exactly as specified and the answer is no." Conflating them makes a client
retry a command that already did its job, which for a non-idempotent operation
is how a compare-and-swap loop turns into a livelock.

**Cost:** Two fields where a naive design has one.

---

## D-0022: Only the most recent result is cached; older sequences are rejected

**Phase:** 1b

**What:** The session table stores one `last_seq` and one `last_result` per
client. A repeat of `last_seq` returns the cached result. A sequence lower than
`last_seq` returns an error.

**Why:** Answering a stale sequence would require retaining every result ever
produced for that client. The alternative -- silently re-applying it -- is
worse than an error: `AnOlderSequenceIsRejectedRatherThanReapplied` shows it
rolling a key back to a superseded value.

A correct client never sends a stale sequence, because it does not advance
`seq` until the previous one is acknowledged. So this error indicates a client
bug, and it should be loud.

**Cost:** A client that loses track of its own sequence numbers cannot recover
by guessing; it must start a new session.

---

## D-0023: The session table grows without bound

**Phase:** 1b. A known gap, recorded rather than fixed.

**What:** Every `client_id` ever seen keeps a session entry forever. There is
no expiry.

**Why not fixed now:** Expiry must be deterministic, because every replica has
to expire the same entries at the same point in the log. A wall-clock TTL is
exactly the wrong mechanism -- replica clocks differ, so they would expire
different entries and diverge. The correct design is expiry by applied-index
age, evaluated at snapshot time, which is why `SessionEntry` already carries
`last_touch_index`.

**Cost:** Unbounded growth proportional to the number of distinct clients over
the cluster's lifetime. Acceptable for a benchmark and a chaos harness with a
fixed client set; not acceptable for a real deployment. It is in the README
limitations section.

---

## D-0024: TakeSnapshot forces a flush and a full compaction

**Phase:** 1b

**What:** `DB::ScanAll` flushes the memtable and compacts down to a single
table, then scans it, taking the first record per user key.

**Why:** A snapshot needs an ordered scan over the memtable and every level,
deduplicated by user key. That wants a merging iterator, which does not exist
yet. Compacting to one table makes the scan trivially correct: records arrive
in ascending user key and descending sequence, so the first record for each key
is its newest version, and a tombstone there means the key is gone.

**Cost, stated plainly:** snapshotting blocks writes for the duration of a full
compaction. If Phase 5 snapshots overlap a Phase 8 benchmark run, this will
appear in p99 and it will be this decision, not the consensus code. The
merging iterator is the fix, and it should be built when a profile shows the
cost rather than pre-emptively.

---

## D-0025: RestoreSnapshot replaces state wholesale

**Phase:** 1b

**What:** `RestoreSnapshot` materializes the entire snapshot, then calls
`DB::DestroyContents()`, then writes the snapshot's contents.

**Why:** Merging a snapshot into existing state keeps keys the snapshot's
author had deleted, so the restored replica silently diverges from the one it
copied. Materializing before destroying matters too: restoring incrementally
would leave the machine half-replaced if a chunk read failed midway, with
nothing to roll back to.

**Cost:** Peak memory of one whole snapshot, and the restore is not
incremental. Phase 5 streams the snapshot over the wire in chunks but still
assembles it in memory here; spilling to a temporary file is the fix when
snapshots outgrow RAM.

---

## D-0026: The consensus core uses generated protobuf types directly

**Phase:** 2

**What:** `src/raft/` operates on `proto::Message`, `proto::Entry`, and
`proto::HardState` from `raft.proto`. There is no parallel set of internal
structs and no conversion layer.

**Why:** The alternative means two definitions of every message plus a
hand-written translation between them, and a conversion bug is both easy to
write and invisible until a real network is involved -- Phase 3 at the
earliest, and possibly not until Phase 10. etcd made the same call.

The determinism argument from D-0018 still holds: the core parses what it
receives and never re-serializes it before acting on it.

**Alternative:** Plain structs with explicit conversion, which decouples the
core from protobuf and would make an alternative transport easier.

**Cost:** The deterministic core depends on protobuf. Acceptable, since the
whole system already does.

---

## D-0027: Ready/Advance makes the durability ordering enforceable

**Phase:** 2

**What:** `ReadyToProcess()` returns hard state, entries to persist, outgoing
messages, and committed entries together. The caller must persist the first two
before sending the third, then call `Advance()`.

**Why:** Figure 2 requires `currentTerm` and `votedFor` to be durable before
responding to any RPC. Expressed as a rule, every call site has to remember it.
Expressed as a data structure handed to one driver loop, there is exactly one
place that can get it wrong -- and the Phase 2 harness deliberately implements
that ordering so Phase 3's real driver has a reference to match.

Reversing the order lets a node vote twice in one term across a crash, which
breaks Election Safety directly.

**Cost:** The core cannot act on its own; something must drive it. That is the
point.

---

## D-0028: CheckQuorum, so an isolated leader steps down

**Phase:** 2

**What:** Once per election timeout, a leader counts the peers it has heard
from. Below a quorum, it becomes a follower.

**Why:** Found by a test that asserted something plain Figure 2 does not
provide. A partitioned leader never learns of a higher term, so it stays in
leader state indefinitely. That is safe for the log -- it cannot commit
anything without a quorum -- but it leaves a node that believes it is the
leader and would happily answer a read with stale data.

My test asserted the minority side has no leader; the implementation was right
by the paper and the test was wrong about what the paper guarantees. Rather
than weaken the test, I implemented the property it was asking for, because
Phase 4 needs it: shrinking the stale-leader window makes lease reads defensible
and cuts failover time.

CheckQuorum does not replace ReadIndex. Phase 4 still confirms leadership with
a heartbeat quorum on every linearizable read; this reduces the window rather
than closing it.

**Cost:** A leader on a briefly flaky link can step down unnecessarily,
triggering an election that was not needed. That is a real availability cost,
paid for a real correctness benefit.

---

## D-0029: The log reports where it truncated, because length comparison is not enough

**Phase:** 2. Found by the randomized harness at seed 5; a genuine bug.

**What:** `RaftLog::TruncateAndAppend` now reports `first_changed`, the lowest
index whose content changed. `HandleAppend` lowers `persisted_index_` to just
below it, so the rewritten entries are re-emitted for persistence.

**Why:** `ReadyToProcess` originally decided what to persist by comparing
`log_.LastIndex()` against `persisted_index_`. That misses the case that
matters. When a follower truncates a conflicting suffix and refills it in the
same step -- which is exactly what log repair does -- the log can end up the
**same length** with **different entries**. `LastIndex() > persisted_index_` is
false, nothing is emitted, and the durable log keeps the entries that were just
discarded. The node then resurrects them on restart.

The failure surfaced hundreds of steps later as a Log Matching violation on a
node that looked healthy, which is why the harness checks invariants after
every step rather than at the end of a run.

An intermediate fix -- clamping `persisted_index_` to `LastIndex()` -- handled
only the case where the log got shorter, and the bug survived it. The lesson
worth keeping: *the length of a log says nothing about its contents.*

**Alternative:** Emit the entire log on every truncation. Correct and wasteful.

**Cost:** One more out-parameter on a hot path.

---

## D-0030: A failing seed is a reproducible bug report

**Phase:** 2

**What:** The randomized soak runs 2000 seeds by default. Setting `RAFTKV_SEED`
replays exactly one and turns the protocol trace on:

```
RAFTKV_SEED=5 ./build/test/raft_cluster_test --gtest_filter='*Randomized*'
```

**Why:** Debugging seed 5 took three wrong hypotheses before the trace showed
what actually happened -- the follower repaired its log correctly and then
reverted on restart, which pointed at persistence rather than at replication.
Guessing was slower than looking, every time.

Invariant failures also dump both nodes' full logs, because "these two byte
strings differ" costs an hour of re-deriving state the harness already had.

**Cost:** 2000 seeds is roughly 13 seconds of the test suite. Worth it.

---

## D-0031: The Raft log fsyncs per Ready batch, not per entry

**Phase:** 3. Deliberately the opposite of D-0014, and the contrast is the point.

**What:** `RaftLogStore::Append` buffers; `Sync()` makes the whole batch
durable. The Ready/Advance contract already forbids sending any message before
its batch is persisted, so one fsync covers every entry that accumulated in
that round.

**Why:** The requirement is to avoid an fsync per entry while never
acknowledging an entry that could be lost. Batching satisfies both because the
batch boundary and the acknowledgement boundary are the same boundary. Under
load the batch grows and the fsync amortises; when idle it degenerates to one
fsync for one entry, which is correct and costs nothing because nothing else is
happening.

The rule, stated so it can be checked: **`Sync()` must return before any
message from that Ready is sent, and before the leader counts its own entries
toward `match_index`.**

**Contrast with D-0014, which must not be confused with this.** The LSM state
machine may lag durability, because anything it loses is replayable from the
Raft log. The Raft log has nothing behind it: an entry acknowledged from here
and then lost is a lost committed write. Same system, opposite durability
rules, and they must never share a file.

**Alternative:** fsync per entry. Simpler to reason about, and it makes write
throughput a function of fsync latency — on this hardware, `F_FULLFSYNC` at
roughly 1 ms, so about 1000 entries/sec.

**Cost:** Correctness now depends on call ordering rather than on a local
guarantee. A driver that sends before syncing would be fast and wrong, and no
unit test of `RaftLogStore` alone can catch it.

---

## D-0032: Hard state is written whole and renamed, never updated in place

**Phase:** 3

**What:** `HardStateStore::Save` writes a temp file, fsyncs its contents,
renames it over the real one, then fsyncs the directory.

**Why:** `currentTerm` and `votedFor` must be durable before any RPC is
answered, and an in-place update of an 8-byte field can in principle tear. The
recovery story for "the term is half of two numbers" is that there is no
recovery story — the node cannot tell which half is real, and a wrong term
breaks Election Safety. Rename is atomic, so the file is either the old state
or the new one.

Both fsyncs are load-bearing. Without the first, the rename can publish a file
whose contents never reached disk. Without the second, the rename itself is not
durable and the directory entry can revert.

An empty `HARDSTATE` is treated as "no state", not corruption: that is what a
crash between create and write leaves, and it is a lost update rather than
damage.

**Alternative:** A small journal of term/vote records. More writes, no better
guarantee for a value this size.

**Cost:** Two fsyncs per hard-state change. Hard state changes on election and
on term change, not per entry, so this is off the hot path.

---

## D-0033: A damaged tail is repaired; damage anywhere else is fatal

**Phase:** 3

**What:** On recovery the log truncates a damaged record only in the **final**
segment. Damage in any earlier segment returns `kCorruption`.

**Why:** A partial record at the end of the last segment is the expected result
of a crash mid-append. Everything before it is intact, and the truncated entry
was never acknowledged, so dropping it loses nothing that was promised.

Damage in the middle is a different thing entirely. Truncating there would
discard every entry in the segments that follow — silently, and those entries
may well be committed. A store that "recovers" by deleting committed data is
worse than one that refuses to start, because the operator never finds out.

**Cost:** A single corrupted byte in an old segment takes the node down until
someone intervenes. That is the correct failure mode: the cluster has other
replicas, and this node's data cannot be trusted.

---

## D-0034: TruncateSuffix rewrites the log rather than seeking

**Phase:** 3

**What:** Dropping a divergent suffix reads every surviving entry, deletes all
segments, and rewrites.

**Why:** Slower than truncating at a byte offset, and chosen anyway. A follower
repairing a divergent suffix is rare — it happens after a leader change with
conflicting entries, not on the steady-state path. A rewrite cannot leave a
half-updated state across a segment boundary, where an in-place truncation has
to delete some segments, truncate one, and update the index consistently; a
crash midway through that leaves a log whose header disagrees with its
contents.

The truncation is fsynced before returning, because a truncation that is not
durable resurrects the discarded entries on restart. That is precisely the bug
the Phase 2 harness caught in the in-memory path (D-0029), and it would be far
harder to spot here.

**Alternative:** In-place truncation with a durable index update.

**Cost:** O(log size) work on a rare path. Revisit if a profile ever shows it.

---

## D-0035: RequestVote and AppendEntries are blocking unary RPCs

**Phase:** 3

**What:** Each handler calls `Step(request)`, drains `Ready`, persists it, then
returns the reply the core produced as the RPC's own response.

**Why:** `HandleRequestVote` and `HandleAppend` each emit exactly one reply
addressed to the sender, synchronously, so the reply can ride the RPC's return
path. That makes "persist before responding" a property of the call structure:
the handler cannot return before `DrainAndPersist` has run. The alternative, a
fire-and-forget `Step` RPC with replies sent as separate messages (the etcd
transport), needs the same ordering enforced separately at every send site.

Heartbeats ride the `AppendEntries` RPC. The receiver dispatches on the
message's own `type` field, not on which RPC method was called.

**Cost:** A handler holds a gRPC thread for the duration of an fsync. Acceptable
at this scale; revisit if Phase 9 shows handler threads as the bottleneck.

---

## D-0036: Commit and apply are different events, and the gap is deliberate

**Phase:** 3. Found by a flaky test, not by design.

**What:** `RaftNode::CommitIndex()` advances inside `Step()`, under the driver's
lock. `ApplyCommitted()` runs after the lock is released. A caller who sees
`CommitIndex() >= n` cannot assume the state machine has applied entry `n`.

**Why it happened:** `ReplicatesAWriteToEveryFollowersDisk` failed about half the
time with "node 1 never applied the write", and the node's applied index was
correct a few microseconds later. The test waited on commit and then read the
state machine. Applying outside the lock is intentional: holding it across an
LSM write would serialise every node's apply behind its own network I/O.

**How to apply:** Anything that needs applied state must wait on
`StateMachine::LastAppliedIndex()`. This is the gap Phase 4's ReadIndex closes
("confirm leadership, wait until applied >= readIndex"), and the reason a read
path that checks `CommitIndex()` would pass most of the time and be wrong.

**Cost:** Two indexes to reason about instead of one.

---

## D-0037: The state machine's applied index can trail Raft's

**Phase:** 3, for Phase 5 to honour.

**What:** `ApplyCommitted` skips `ENTRY_NOOP` and `ENTRY_CONFIG`. A new leader's
no-op therefore advances Raft's commit index without advancing the state
machine's applied index.

**Why it matters later:** `KvStateMachine::TakeSnapshot(index, term)` requires
`index == LastAppliedIndex()`. A Phase 5 trigger that snapshots at Raft's commit
index will be refused whenever the newest entry is a no-op. The trigger must
snapshot at the state machine's own applied index, and compact the Raft log to
that.

**Alternative:** Feed no-ops through `Apply` as empty commands. Rejected: it
would need `KvStateMachine` to special-case "no command" to avoid treating it as
malformed, bending Phase 1b code to a Phase 3 concern.

**Cost:** A known interaction waiting for Phase 5. Nothing calls `TakeSnapshot`
from the driver yet, so nothing is broken today.

---

## D-0038: Outbound dispatch is a detached thread per message

**Phase:** 3. A known weakness, recorded so Phase 9 starts from the truth.

**What:** `DispatchOutbound` starts a detached thread for each message.
`inflight_count_` plus a condition variable lets `Stop()` wait for them.

**Why:** The tick loop must never block on a peer, or one slow follower stalls
replication to the rest. A thread per message is the simplest thing that
guarantees that.

**What is wrong with it:** There is no per-peer in-flight cap. A leader ticking
every 15-50 ms with a 5x-tick deadline can have several requests outstanding to a
slow peer, each on its own thread, and responses can complete out of order. Raft
tolerates the reordering and `TruncateAndAppend` tolerates the overlap, so this
is a resource problem rather than a correctness one. Phase 9's pipelining work
needs a bounded window per follower, and that replaces this wholesale.

---

## D-0039: Never truncate a log file a live process has open

**Phase:** 3. A test-script bug, recorded because it will recur.

**What:** `scripts/subprocess_cluster_test.sh` first reset survivor logs with
`: > file` to separate pre-kill from post-kill leader lines. The election worked;
the detection did not.

**Why:** A running process keeps its own file offset. Truncating the file from
outside leaves that offset where it was, so the next write lands past a hole of
NUL bytes. `file` reports the log as binary and `grep` stops matching lines after
the hole. The script now never touches a live log and instead requires the new
leader's id to differ from the killed leader's.

**Related:** the same script also hit bash 3.2's missing associative arrays.
macOS ships bash 3.2, so scripts here avoid `declare -A`, `mapfile`, and empty
arrays under `set -u` (see the comments in `scripts/ci.sh` and
`scripts/format.sh`).

---

## D-0040: ReadIndex acks are tied to the request, and the read point is fixed at request time

**Phase:** 4

**What:** `RaftNode::ReadIndex(ctx)` records the commit index, sends heartbeats
carrying `ctx`, and serves the read only after a quorum of responses *echoing
that ctx*. The read point is the commit index when the request started, not when
the quorum answered. A leader that has not yet committed an entry from its own
term refuses (`kUnavailable`); a follower refuses (`kNotSupported`).

**Why each piece:**
- *Request-specific ack.* A heartbeat response proves leadership only as of when
  the matching heartbeat was sent. An ack from before the read arrived may
  predate a newer leader's election.
- *Own-term gate.* A fresh leader's commit index can lag what the previous
  leader already told clients was committed. This is the Figure 8 hazard applied
  to reads. A deleted gate produced a stale read in the randomized soak (seed 14).
- *Read point at request time.* Everything committed before the read began is at
  or below that index. Moving it later would be unnecessary, and moving it
  earlier would be stale.

**Mutation testing, and what it showed.** A read test that cannot fail proves
nothing, so three deliberate bugs were introduced:

| Mutant | Caught by |
|---|---|
| serve without waiting for a quorum | 4 tests, including the partition test and the soak (stale read at seed 42) |
| drop the own-term-commit gate | the unit test and the soak (stale read at seed 14) |
| accept any heartbeat ack, ignoring the context | **only the hand-written unit test** |

The third result is a real limit, not a pass. The simulator drops in-flight
messages the instant a partition forms, so "a stale ack from before the
partition arrives after the read request" cannot occur there. The
request-context property is therefore protected by one unit test
(`NeedsAQuorumOfAcksForThisRequest`), not by the soak.

**Single-node clusters** needed a fix found along the way: the term-start no-op
never committed until a client write happened to trigger `MaybeAdvanceCommit`,
so `ReadIndex` on an idle one-node cluster returned "unavailable" forever.

**Not done:** lease reads (the spec's optional fast path). Every read costs a
heartbeat round trip to a quorum.

---

## D-0041: A heartbeat response triggers a resend to a lagging follower

**Phase:** 4. A liveness bug present since Phase 2.

**What:** On `MSG_HEARTBEAT_RESP`, if the follower's `match_index` is below the
leader's last index, the leader calls `SendAppend`.

**Why it was missing:** AppendEntries were only sent on a client write or on a
response. If an AppendEntries, or its response, was lost, the follower stayed
behind until the *next proposal* happened to broadcast. When every writer is
blocked waiting on that very commit, no further proposal comes, and replication
stalls permanently.

**How it surfaced:** the 8-writer gRPC test failed 13 of 20 runs with
`TimedOut`, always in multiples of 8 (one stuck operation per writer thread).
Over real gRPC the 75 ms RPC deadline expires during a follower's fsync and the
response is dropped. The Phase 2 simulator never caught it because every
scenario kept proposing.

**Regression test:** `ALostAppendResponseIsRetransmittedWithoutNewWrites` drops
exactly the `MSG_APPEND_RESP` messages, stops dropping them, proposes nothing
more, and requires the entry to commit. It failed before the fix and passes
after; it is deterministic.

**Cost:** while a follower is merely slow, every heartbeat resends up to
`max_entries_per_append` entries. Part of the same unbounded-in-flight problem
as D-0038.

---

## D-0042: Committed entries are applied through one ordered queue

**Phase:** 4. A race in the Phase 3 driver.

**What:** `DrainAndPersist` pushes committed entries onto `apply_queue_` under
`mutex_`; `ApplyPending` drains it under a separate `apply_mutex_`.

**Why:** the original driver applied "what this thread just drained", after
releasing the lock. Two threads can drain `[5]` and `[6]` and race to apply them.
If 6 lands first, `KvStateMachine` records 6 as applied and then discards 5 as an
already-applied duplicate. A committed, acknowledged write is silently lost.

**Evidence, corrected.** An earlier version of this entry said the concurrent
writers test could see this bug: with the apply mutex removed it lost writes on a
follower in 3 of 12 runs. That number was measured while the driver was also
re-delivering the entire committed log on every drain (D-0047), which massively
amplified concurrent applying. On the corrected driver the same mutant is caught
**0 of 40** times, so for a while this guarantee had no test that would notice it
breaking.

`RaftDriverTest.ApplyIsSerialisedAndOrderedUnderConcurrentProposals` now covers
it: a one-node driver and a fake state machine that flags overlapping or
non-increasing `Apply` calls. Mutating away the apply mutex is caught 15 of 15.
It took two attempts to get there: with a fake `Apply` cheaper than the fsync the
proposer holds the lock across, the race cannot occur and the test passed even
with the mutex gone, so the fake's cost is deliberately larger than an fsync.

---

## D-0043: Reads wait on the driver's applied index; every consumed entry advances it

**Phase:** 4. D-0037 coming due.

**What:** the driver keeps `applied_raft_index_`, advanced for *every* entry it
processes including no-ops. A `LinearizableGet` waits on that, not on the state
machine's own index. Separately, `KvStateMachine` now writes the applied index
even when a command is answered from the session cache or rejected
(`AdvanceOnly`).

**Why:** the state machine skips no-ops, so its index can trail Raft's. A read
pinned at a no-op's index (an idle cluster: the term-start no-op is the only
entry) would wait forever. `AReadOnAnIdleClusterDoesNotWaitForeverOnANoop` pins
this. Deduplicated and rejected commands used to return without advancing the
index, which would have made a snapshot at Raft's index refuse; that case is
fixed here. No-ops still do not reach the state machine, so the Phase 5
snapshot trigger must still snapshot at the state machine's own index.

---

## D-0044: A reused sequence number carrying a different command is an error

**Phase:** 4. Found by my own test script, which first looked like data loss.

**What:** each session entry stores an FNV-1a digest of the command bytes. A
repeat of the cached `seq` is answered from the cache only if the digest
matches; otherwise `InvalidArgument`.

**What happened:** the real-process `SIGKILL` script reported 9 of 10
acknowledged writes missing after the kill. They had never been applied. Each
`raftkv_cli` invocation is a separate process, and I had given them all
`--client-id smoke`, so every write was `(smoke, seq=1)`. The state machine
correctly treated writes 2-10 as retries of write 1 and answered `OK` from its
cache. With a distinct id per invocation, all 10 survive the kill, and the
restarted node reads 11 log entries back from its own disk.

**Why it still needed fixing:** the server acknowledged nine writes it never
performed, with no error anywhere. A real client that restarts with the same id
and a reset counter loses writes silently. A retry is byte-identical, so content
can tell the two apart where the sequence number cannot.

**Residual risk, stated plainly:** the check catches a reused number with a
*different* command. It cannot catch a restarted client re-sending the *same*
command under a reused number, which is indistinguishable from a retry. The
safe usage is a fresh `client_id` per process lifetime (the client library's
default) or a persisted counter. Sessions are also still never expired.

**Cost:** 8 bytes per session; one extra applied-index write on dedup paths.

---

## D-0045: The client serialises its own operations and reports unknown outcomes honestly

**Phase:** 4

**What:** `KvClient` assigns `seq` once per logical operation and reuses it for
every retry; takes one operation at a time per instance; follows `NOT_LEADER`
hints without backoff; backs off with jitter otherwise; returns `kTimedOut` when
the budget runs out.

**Why:**
- *One at a time.* A session keeps only the latest result, so two operations in
  flight under one id could have the later one's sequence number overtake the
  earlier one's retry. Parallelism means more clients.
- *`kTimedOut` is not failure.* For a write it means "may have been applied".
  Anything that records histories for a linearizability check must log it as
  indeterminate, never as a failed operation, or the checker will reject correct
  histories.
- *Hints are free.* Being told where the leader is means nothing is wrong, so
  waiting would only add latency.

---

## D-0046: Under heavy concurrent writes, heartbeats starve behind fsync (known, deferred)

**Phase:** 4. Not fixed; recorded so Phase 9 starts from it.

**What:** `DrainAndPersist` holds the driver's global lock across the fsync, and
the ticker needs that lock. With 8 writers each forcing a sync, heartbeats arrive
late enough that followers' election timers fire: one run lost leadership
mid-test (`NotSupported: not the leader`). The concurrency tests raise
`election_tick` to 60 to measure replication ordering rather than lock
contention.

**Fix, and it is Phase 9's headline:** group commit. One fsync should serve many
proposals, and the sync should not hold the lock the ticker needs.

---

## D-0047: A moved-from protobuf made the core re-emit its whole committed log

**Phase:** 4. A bug I introduced while rewriting the driver, and the real cause of
the "acknowledged write missing on a follower" failures.

**What:**
```cpp
for (auto& entry : ready.committed_entries) apply_queue_.push_back(std::move(entry));
...
node_->Advance(ready);   // reads ready.committed_entries.back().index()
```
`Advance` learns how far the core has handed out from the last committed entry's
index. A moved-from protobuf reads as index 0, so after every drain the core
believed it had applied nothing and re-emitted the entire committed log from
entry 1 on the next one. Fixed by copying; `Advance` now sees the real index.

**Why it went unnoticed.** The state machine's index-based dedup (`index <=
last_applied`) silently absorbed every repeat, so data stayed correct and all 197
tests passed. The only symptoms were O(history) wasted work per commit and the
driver's applied index sweeping 1..N over and over, so a read of it could land
anywhere in that sweep.

**How it was found.** The stress test failed about 1 run in 150 with a follower
"missing" a write. A captured timeline showed `target=265` against
`leader_commit=321` and the leader's applied index reading 265, then 321, then
287. Nothing was lost: the follower was two entries behind, and the test's
catch-up gate had compared it to a transiently low target. Earlier guesses (apply
reordering, lost wakeups, session dedup) were each plausible and each wrong;
reading the numbers was faster than reasoning.

**What now guards it.** `RedeliveredEntries()` counts any committed entry handed
to the apply step twice or out of order; the driver logs it at ERROR and tests
assert it is zero. Re-introducing the `std::move` is caught 10 of 10 runs.

**What I got wrong along the way.** I suspected this bug also explained the
heartbeat starvation under load. Measured, the stress test takes the same ~6 s
before and after the fix, so it did not: the cost there is the per-write fsync
under one lock (D-0046).

**On the evidence.** 150 clean runs after the fix is *not* the proof, since the
failure rate was about 1 in 150 and a clean 150 would occur by chance roughly a
third of the time. The proof is the mechanism, the counter, and the mutation
results above.

---

## D-0048: Snapshots are installed whole, and the leader compacts right up to its snapshot

**Phase:** 5.

**What:** when a follower's `next_index` falls below the leader's first log index,
the leader sends `MSG_SNAPSHOT` (one message, the whole state machine image).
The follower persists it (atomic file + directory fsync) *before* its hard state,
resets its log to start after the snapshot, and only then acknowledges. The
driver snapshots every `snapshot_threshold` applied entries and compacts the log
to that index.

**Why persist the snapshot first:** the hard state records a commit index. If the
node crashed after writing a commit index covering a snapshot it never saved, it
would believe it held committed data it does not have. Recovery also checks the
log against the snapshot's term and resets it when they disagree, so a crash
between the two writes cannot leave a log that contradicts the snapshot.

**Costs, stated plainly:** the image is built in memory and sent in one message
(gRPC limits are lifted), so this is wrong for gigabytes. And compacting exactly
to the snapshot means a follower only slightly behind is sent a snapshot instead
of a few entries; a retention margin would need the core to separate "snapshot
index" from "first retained index". A leader does not resend a snapshot to the
same follower for `snapshot_resend_tick` ticks, so a slow follower is not buried
under copies.

---

## D-0049: Multi-key writes are one Raft entry with preconditions evaluated before the writes

**Phase:** 6 (SQL layer groundwork).

**What:** `Batch` carries checks (key must be absent, or equal a value) and ops
(put/delete). All checks are evaluated against the state *before* the batch; if
any fails nothing is written and the first failing check is reported by position.

**Why before, not during:** if checks could see the batch's own writes, the result
would depend on the order of ops, and a batch could satisfy its own precondition.
The price is that two rows with the same key in one batch both pass an
"absent" check; the SQL layer rejects duplicates inside a statement itself.

**What it is not:** atomic *per Raft group*. `ShardedClient::Batch` groups keys by
owner and applies each group as its own entry, in shard order. Earlier groups stay
applied if a later group's check fails, and are reported so the caller can undo
them. Cross-shard atomicity needs transactions and is not provided.

---

## D-0050: A group enforces the range it owns inside the replicated apply

**Phase:** 6.

**What:** each group persists an owned range, an optional fence, and an epoch in
its state machine (so they travel with snapshots). Writes outside the owned range
are refused as `WRONG_SHARD`, writes inside a fence as `MOVING`. Both decisions are
made in `Apply`, so every replica accepts and refuses exactly the same writes
either side of a handover. Reads are judged after the ReadIndex wait, when the
node's view of its range is guaranteed current.

**Why enforce on the server at all:** a client's shard map can always be stale. If
only clients knew the layout, a stale client would write to the old owner and the
data would be lost or diverge. Making the group the authority turns a stale map
into a retryable error instead of corruption. A stale epoch is refused, so a
slow coordinator cannot undo a newer handover.

---

## D-0051: Splitting a shard: release the source before activating the target

**Phase:** 6.

**What:** `ShardSplitter` runs fence, copy, publish, release, activate, purge,
recording progress in the directory after each step. Every step is idempotent, so
a dead coordinator is resumed by a new one repeating the step it was on.

**The ordering that matters:** *release* (source stops serving the range) must come
before *activate* (target starts serving it). Reversed, a client holding the old
map could read a stale value from the source after the target had accepted a newer
write. In the order chosen, the moving range is briefly served by nobody, which is
a pause rather than an inconsistency. Between fence and release the source still
serves reads of the range but refuses writes, and the target refuses everything,
so no value can change while two groups hold copies.

**Resuming:** each coordinator instance takes a fresh client id, because the groups
remember the last sequence number per client and would reject a lower one from a
restarted coordinator. Tests stop a coordinator after every step and have another
finish the split.

**Limits:** one split at a time per source shard is assumed. The directory's
compare-and-swap keeps two coordinators from publishing conflicting maps, but it
does not stop both from fencing the same source. Purge is capped per command and
repeated, and refuses any range overlapping what the group still owns.

---

## D-0052: Cross-shard transactions are two-phase commit with locks in the shards

**Phase:** 6.

**What:** a transaction locks every key it touches (writes, and keys it only read)
on every shard involved, validating that each key it read still holds the value it
saw. Committing the *primary* key (the smallest) is the single decision. The other
shards are then rolled forward, and anyone who meets a lock left by a dead
coordinator asks the primary's shard what was decided and finishes the job. All of
it is replicated state, so a lock or a decision survives a leader change.

**Choices and why:**
- *Locks live in the data shard, in its own Raft log.* A lock is then exactly as
  durable and as ordered as the data it guards, and no external lock service can
  disagree with it.
- *Readers and writers both respect locks.* A reader that finds an intent lock
  settles it before answering, so a half-committed transaction is never visible.
  A lock that only holds a key the transaction *read* is invisible to readers and
  only blocks writers.
- *Rollback leaves a marker.* Without it, a delayed prewrite from a transaction
  someone already rolled back would create a lock nobody will ever clear.
- *Whoever reaches the primary first decides.* A contender that gives up waiting
  rolls the transaction back at the primary; if the coordinator's commit got there
  first, the rollback learns it already committed. No timestamps or clocks are
  involved, so clock skew cannot cause a wrong answer, only a slow one.
- *Reads are validated, not versioned.* This is optimistic concurrency, not
  snapshot isolation: a transaction either commits as if it ran alone, or reports
  that something it read changed and the caller retries.

**Tested by killing the coordinator** at the two points that matter (after
prewrite and before the decision: the transaction must vanish; after the decision
and before the other shard hears: it must be rolled forward by the next reader).
Breaking validation, and breaking roll-forward, each fail the tests that should
catch them.

**Costs and limits, stated plainly:**
- Scans are not validated, so a transaction cannot protect a *range* against
  phantom inserts. SQL gets statement-level atomicity, not `BEGIN`/`COMMIT`.
- An abandoned lock blocks its keys for `lock_wait` (300 ms by default) before
  someone rolls it back.
- Decision markers are never garbage-collected.
- A range with a lock in it cannot be fenced for a split until the lock settles;
  the split fails and is retried.
- Contended keys retry a lot: 48 racing transfers over four accounts needed 34
  retries in the test. That is correctness, not speed.

---

## D-0053: The commit index is no longer written to disk on every advance

**Phase:** 6 (found by benchmarking).

**What:** `ReadyToProcess` used to emit a hard-state write whenever term, vote *or
commit* changed. It now writes only when term or vote change. The commit index is
still stored whenever the hard state is written for those reasons, so the stored
value never goes backwards.

**Why it is safe:** in Raft the commit index is volatile. A restarted node
relearns it from the leader (an AppendEntries or heartbeat carries it), and a
stale *lower* value is always safe, because commit only ever moves forward. What
must be durable before answering an RPC is the term and the vote (Figure 2), and
those still are. A restarted node whose stored commit is stale re-emits a few
entries to the state machine, which dedups by index. A restored snapshot sets the
commit index itself.

**Why it was worth it:** every replicated write caused a hard-state save on the
leader (temp file, full fsync, rename, directory fsync) *on the path to the
client's acknowledgement*, and another on each follower. An alternating A/B of the
old and new binaries (`results/50`) shows one-key writes going from a median of 38
to 72 ops/s and 100-key batches from 3.6K to 7.7K keys/s, with write p50 falling
from about 25 ms to about 12 ms.

**How I checked it, and what I did not:** the full suite passes (299 tests),
including 2,000 randomized crash/restart/partition scenarios per run that restart
nodes with only their persisted state. I did not model-check it, and one unrelated
driver test timed out once in a full run that was also unusually slow overall; it
passed 15 of 15 times alone and on a rerun of the whole suite, so I'm treating it
as a flake but have not found its cause.

**What it does not fix:** writes are still one fsync pair each and still do not
scale with concurrent writers (87 to 145 ops/s from 1 to 16 threads). Group commit
is the next lever.

---

## D-0054: Group commit: proposals no longer fsync under the driver lock

**Phase:** 6 (follow-up to D-0053).

**What:** `ProposeAndWait` and `Propose` now append to the in-memory log under
`mutex_` and then call `FlushProposals`. The first caller takes the pending
`Ready`, sets `persisting_`, releases `mutex_` for the log append and fsync, and
re-takes it to `Advance`. Writers that arrive during the fsync append their
entries and wait; the next flush syncs all of them in one call.

**Why it is safe to release the lock:** the only thing another thread can do to
the node during an unlocked persist is *append* (a leader's `Propose`), which
cannot invalidate entries already handed out. Everything that could truncate or
reorder the log (any `Step`, `Tick`, `ReadIndex`, snapshot compaction) first waits
for `persisting_` to clear, so a follower cannot drop entries that are mid-write.
`Advance` marks stable by the index of the last entry *in that Ready*, so entries
appended during the sync are simply picked up by the next one. Messages and
committed entries in a Ready are copied before the unlock and handed out only
after the sync, so nothing is acknowledged or applied before it is durable.

**Measured:** alternating A/B, 4 rounds (`results/52`), medians: 16 writers 140 to
309 ops/s (p50 113 to 52 ms), 4 writers 98 to 118, 1 writer unchanged (88 to 85,
within run spread). 100-key batches at 4 threads 9.4K to 10.7K keys/s.

**Tests:** a new driver test (16 threads, 160 writes, single node) requires every
write applied exactly once and at least 15% fewer syncs than writes; with the
unlock removed it fails (158 syncs for 160 writes). The full suite passes under
ASan, UBSan and TSan (`results/53`).

**What it does not do:** followers still sync once per AppendEntries, replication
is not pipelined beyond the per-peer cap, and closed-loop writers drift out of
step so the average batch is far below the thread count. Throughput is about 2x at
16 threads, not 16x.

---

## D-0055: A linearizability checker and a kill campaign

**Phase:** 10 (the checker DECISIONS earlier deferred).

**What:** `test/linearizability.h` checks a history of puts, gets and
compare-and-swaps on independent registers: per key, a depth-first search for a
total order that respects real time, memoised on (set of placed operations,
register value). Operations with unknown outcome (client timeout) have no return
time, so they constrain nothing and may be placed after their start or left out.
`linearizability_cluster_test` drives it against a real 3-node cluster with
concurrent clients and random node kills, including double faults.

**Why the checker can be trusted:** 13 unit tests cover histories it must
reject (stale read, lost acknowledged write, reads going backwards, two
swaps both winning, an unknown write applied twice) and accept. Separately,
removing the read-point wait from `AwaitReadPoint` makes the campaign fail on
every seed tried (4 of 4) with a readable counterexample.

**What it did not catch:** disabling exactly-once deduplication passed all 4
seeds tried. Duplicates need a retry after an applied-but-unacknowledged write,
which this schedule rarely produces. That property is covered by the state
machine and driver tests, not by this campaign.

**Limits:** kills are `CrashNode` (driver stopped, server shut down, directory
kept), not power loss or SIGKILL mid-fsync; there are no network partitions,
clock faults or disk errors. Fifteen clean seeds (`results/54`) is evidence, not
proof.

**Found along the way:** the test fixture dereferenced a null driver when a node
restarted while another was down. Fixed in the fixture; no production code
changed.

---

## D-0056: Truncate the durable log when a follower's suffix is replaced

**Phase:** 10 (found by the process chaos test).

**What was wrong:** when a follower accepted a different leader's entries over a
divergent suffix, `RaftNode` rewound its persisted index and re-emitted the
overwritten indices. The driver passed them to `RaftLogStore::Append`, which
only accepts index `last + 1`, so every persist failed with `append at index N
leaves a hole after M`. The in-memory log was already changed, the disk was
not, and the node repeated the failure on every tick: alive, never serving, never
recovering. `RaftLogStore::TruncateSuffix` existed and was tested, but the driver
never called it. The in-process Raft harness models storage without this
interface, and the gRPC cluster tests rarely produce a conflicting suffix, so
nothing exercised the gap until SIGKILLs and partitions did. One run left 830 of
1,005 client operations with unknown outcomes.

**Fix:** `PersistReady` calls `TruncateSuffix(first)` when the first entry to
persist is at or below the log's last index, then appends. A deterministic driver
test (term-1 entries 1..3, then a term-2 leader overwriting from 2, then a
restart) failed with the same error before the change and passes after.

**Second bug in the same path:** `TruncateSuffix` deleted every segment and
rewrote the survivors, so a crash in the middle lost the entire log, entries the
node had acknowledged included. It now deletes later segments newest first and
cuts the holding segment at the entry's byte offset, so at every step the file
set is a valid prefix of the old log; what a crash can cost is entries the
leader is about to overwrite, which can never have been committed. A storage test
truncates across segments at four cut points and reopens.

**What I could not explain:** two earlier runs of the process campaign (before
the fix) reported `no valid order` histories. I did not capture their full
output, they did not reproduce afterwards, and twelve seeds after the fix pass. A
node that failed to persist while its in-memory log had advanced is a plausible
cause, but I did not show that. Treat it as an open question, not a closed one.

**Evidence:** `results/55`: 12 of 12 seeds pass after the fix; with reads made
non-linearizable the process campaign fails 3 of 3.

**Limits:** `SIGKILL` and cut TCP relays on one machine. The page cache survives
a process kill, so this does not test power loss; there is no latency, reorder or
asymmetric-loss modelling.

---

## D-0057: An unbounded scan was judged as a lookup of the empty key

**Phase:** 6 (found while testing the balancer).

**What was wrong:** `CheckRange` and `FindBlockingLock` read `start == end` as "a
single key". A scan of the whole key space is `start = ""`, `end = ""` (unbounded),
so it was judged as a lookup of the key `""`: owned, no lock in the way. A shard
that had just given away the top of its range therefore answered an unbounded
scan with only the keys it still held, returned no error and no "more" flag, and a
client holding the pre-split map accepted that as the whole answer. In the test,
160 keys came back as 75. The same ambiguity meant an unbounded scan skipped the
transaction-lock check entirely, so it could read past a lock it should have
waited on or resolved.

**Fix:** `CheckSpan` and `FindBlockingLockInSpan` always treat their arguments
as a range; `LinearizableScan` uses them. The point forms keep their meaning.
Unit tests pin both cases; the balancer test, which reads through a client with a
stale map right after a split, failed with 75 of 160 before the change.

**Why earlier tests missed it:** the existing split tests do a `Get` first, which
refreshes the client's map, and only then scan.

---

## D-0058: A key-count balancer on top of the splitter

**Phase:** 6.

**What:** `ShardBalancer::Plan` scans each shard's range (linearizable pages, up
to `max_keys_per_shard + 1` keys) and, for the first shard over the limit, picks the
median counted key; `RunOnce` takes a spare group's addresses from a callback and
runs the existing `ShardSplitter` with an id derived from the map epoch.

**Why so simple:** the hard parts (fence, copy, publish, release, activate, purge,
and resuming after a crash) already exist. The open question was only *when* and
*where*, and a key count with a median cut answers both well enough to show the
mechanism end to end.

**Tested:** a shard under the limit is left alone and no spare group is consumed;
an oversized shard is split at its median with all 160 keys intact and in order,
and a second pass does nothing; writes issued during the split are all
acknowledged and all present afterwards.

**Not done:** it is not scheduled; it counts keys, not bytes or request load; it
holds one shard's keys in memory while planning (`max_keys_per_shard` bounds
that); it never merges shards; and a skewed key distribution can make the median of a
*prefix* of the shard a poor split point if the count stops early.

## D-0059: Storage is an interface; lsmtree is a second engine behind it

**Phase:** post-10, building on the standalone engine
(github.com/rohangarim/embedded-lsm-kv).

**What:** `src/engine/engine.h` is the whole contract the state machine needs:
`Get`, an atomic `Write(Batch)`, `Scan[lo,hi)` with a limit, `ScanAll` (a
consistent image), `SyncWal` (a durability barrier) and `DestroyContents`.
`RAFTKV_ENGINE=builtin|lsmtree` (or `--engine`) picks the implementation. The
original mini engine moved from `src/lsm/` to `src/minilsm/` and is the
`builtin` adapter; the user's engine is the `lsmtree` adapter. lsmtree is
fetched by full commit SHA (`RAFTKV_LSMTREE_SHA`, `scripts/fetch_lsmtree.sh`)
and its headers are visible to exactly one translation unit
(`lsmtree_engine.cc`, pimpl).

**Why the move:** lsmtree's include prefix is `lsm/` and its namespace is the
global `lsm`; ours was also `src/lsm/`. Two trees with the same prefix cannot
share an include path, and renaming a dependency's include directory is worse
than renaming our own. The C++ namespace stays `raftkv::lsm`; only the
directory and 53 include sites in 32 files changed. This is a finding for the
lsmtree owner too: a more specific include prefix would have avoided it.

**How it is checked:** `engine_conformance_test` runs the same cases against
every available engine: reopen recovery, `SIGKILL` after a barrier, scan
bounds/limits/ordering, deletes, binary and prefix keys, random operations
against a `std::map`, readers during `DestroyContents`, a reader never seeing
half of a batch (D-0060). `state_machine_test` is parameterised over the engine
too. `scripts/ci.sh` reruns each preset's suite with `RAFTKV_ENGINE=lsmtree`.

**Not done:** `ScanAll` is a pure virtual rather than a `NewSnapshot()` handle.
The driver still holds `apply_mutex_` while it takes a Raft snapshot (sync,
serialise, fsynced save, log compaction), so an engine whose scans do not block
writers does not yet make snapshots non-blocking end to end. That needs
`Engine::NewSnapshot()` plus a driver restructure; it is the next step (read
from the code, not measured). D-0037 still applies: the snapshot trigger must
use the state machine's own applied index.

## D-0060: lsmtree publishes a batch's sequence before its writes: torn reads

**Phase:** post-10.

**What was found:** in the pinned lsmtree commit `DB::Write` advances
`last_sequence_` by the batch size *before* inserting the batch's entries into
the memtable. LevelDB publishes after. A reader that takes its snapshot
sequence in between can see some of a batch's keys and not others, which breaks
the "a batch is atomic" property the state machine depends on (an applied
command and its session record are one batch).
`AReaderNeverSeesHalfOfABatch` fails on lsmtree without the guard below.

**What the adapter does:** a reader-writer lock around the engine: `Write` takes
it exclusively, `Get` shared; scans and `ScanAll` pin their iterator and then
iterate without holding it. `kGuardReadAtomicity` in `lsmtree_engine.cc` turns
it off. Mutation check: with it off, `AReaderNeverSeesHalfOfABatch/lsmtree`
fails; with it on it passes.

**What the mutation check did NOT show:** `KvClientTest.
ConcurrentScansNeverSeeHalfOfAnAtomicBatch` (end to end) passed 3 of 3 with the
guard off. The replicated path does not expose the torn read today, most likely
because scans and applies are serialised by the driver. So the guard is
defence for the engine's own contract, and the end-to-end test guards the
property but is not evidence that the guard is needed.

**Cost of the guard:** every write takes an exclusive lock. End to end, 1-key
puts were 0.83x (1 thread) and 0.81x (16 threads) of builtin in
`results/61_raftkv_bench_ab_engines.txt`, consistently across all three rounds.
Whether the guard is the cause was not isolated.

**For the lsmtree owner:** publish `last_sequence_` after the memtable inserts
(a one-line move); `AReaderNeverSeesHalfOfABatch` in
`test/engine_conformance_test.cc` is a ready regression test. Then the guard
can be deleted.

## D-0061: What each adapter promises, and what the engines measured

**Phase:** post-10.

**Semantics chosen.** `SyncWal` on lsmtree is `FlushMemTable()`: lsmtree has no
public WAL sync, and a flush is a correct (if heavier) barrier because
rotating the memtable closes the old WAL. Apply-path writes use the `kNever`
sync policy, the same bargain as D-0014: the Raft log is the durability
backstop and the driver calls `SyncWal` before it compacts the log. An empty
batch returns early and syncs nothing. `DestroyContents` waits for pinned
iterators, removes the directory and reopens. Memtable sizes at or below about
4 KiB make lsmtree flush on every write (its arena allocates 4 KiB blocks), so
tests use 8 KiB and above. Also for the lsmtree owner: a native `SyncWal()`, and
the 40-59 ms an empty-directory `Open` takes (builtin: 0.3 ms), which I
attribute to its fsyncs but did not isolate.

**Engine level** (`results/60_engine_bench_ab.txt`: 3 rounds, builtin and
lsmtree alternating within each; load before each run 4.6-6.2 on 10 cores,
shared machine; 200,000 keys, 100 B values, 4 threads where threaded). Medians:

| | builtin | lsmtree |
|---|---|---|
| open empty dir | 0.32 ms | 40-59 ms (all three) |
| 3-op atomic batch, 1 thread | 74,380 /s | 129,877 /s |
| point get, 1 thread | 137,594 /s | 513,344 /s |
| point get, 4 threads | 305,402 /s (205k-311k) | 698,299 /s (680k-918k) |
| scan 100 rows, bounded | 1,047,458 rows/s | 4,504,261 rows/s |
| scan 100 rows, open-ended | 1,155 rows/s | 4,761,352 rows/s |
| full 200,065-row snapshot scan | 1.508 s | 0.040 s |

The open-ended scan is slow on the builtin engine because it materialises the
whole range before returning the first 100 rows. The 4-thread get numbers moved
a lot with machine load (an earlier run at load ~3 gave 381k vs 1.49M), so only
the spread, not a ratio, is a safe claim there.

**Not claimed:** the builtin writer's worst single-batch latency was 1.0-1.2 s
in every window, *including* "writer alone", so those stalls are not caused by
the concurrent scan and their cause was not isolated. lsmtree's was 4-13 ms.

**End to end** (`results/61_raftkv_bench_ab_engines.txt`, three nodes, 3 rounds
alternating, load 3.6-7.1): 100-row scans 59,875 vs 508,178 rows/s (1 thread,
8.5x) and 145,586 vs 985,285 (4 threads, 6.8x). Linearizable gets 1.02-1.05x
and transactions 0.91-1.01x, i.e. no difference outside noise: those are
fsync- and round-trip-bound. Puts were *slower* on lsmtree (see D-0060).

**A conclusion retracted.** An early non-interleaved run suggested lsmtree was
flaky under the test suite. Interleaved, builtin and lsmtree failed
indistinguishably (Fisher exact p = 0.58, computed in-session; the raw counts
were not committed, so treat the figure as unverified), and the hypothesis that
Open's fsyncs caused it was tested and refuted. The conclusion
was an artefact of running one engine's suite first and the other's later while
load drifted. `scripts/ab_engines.sh` exists so that cannot recur.

## D-0062: Two shutdown races the second storage engine made visible

**Phase:** post-10.

**What happened:** the node-kill campaign (D-0055) crashed with `SIGSEGV` on
seed 3 under lsmtree in 2 of 6 runs. lsmtree opens far more slowly than the
builtin engine, which widens the window in which a restarting node's gRPC
server is already listening with no driver attached.

1. `RaftGrpcService::RequestVote` dereferenced its driver without the null check
   the other two handlers had (stack: `RequestVote` ->
   `RaftDriver::HandleRequestVote` with a null `this`). D-0055 recorded a null
   driver as a fixture-only bug; this was the production handler. Fixed: each
   handler reads the pointer once through `std::atomic_load` and answers
   `UNAVAILABLE` when it is empty. `RaftGrpcServiceTest.
   RpcsBeforeTheDriverIsAttachedAreUnavailable` covers it; with the check
   removed from `RequestVote` the test process dies, with it restored the test
   passes.
2. After that fix, one run of seed 2 on the *builtin* engine aborted with
   `bad_weak_ptr`. The crash report's stack: a `DispatchOutbound` sender thread
   in `RaftDriver::SendOne` creating a call on a gRPC channel that was being
   destroyed. `Stop()` waited for in-flight senders, but an inbound handler can
   still run after that wait (the fixture stops the driver before the server)
   and start a new detached sender. Fixed: `Stop()` sets `stopped_` under
   `inflight_mutex_`, and `DispatchOutbound` drops messages once it is set.

**Evidence, honestly:** (2) was seen once in about 60 runs and did not
reproduce in 25 further plain runs or 12 under a debugger, so there is no
before/after rate. The case for the fix is the stack plus the ordering argument
(a sender is counted under the lock before it starts; one running after
`Stop()`'s wait must have been counted after it). There is **no deterministic
regression test** for (2): provoking it needs a leader emitting outbound traffic
from an inbound handler after `Stop()`.

After both fixes: seeds 1-6 on both engines plus 6 more rounds of seeds 2 and 3
(36 runs, engines alternating, load 2.6-5.2) all passed
(`results/63_linearizability_both_engines.txt`). With failures at roughly 1 in
60 to 1 in 3 depending on the bug, a clean streak this size is weak evidence;
the stacks and the ordering argument are the real case.
