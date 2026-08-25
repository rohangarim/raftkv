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
