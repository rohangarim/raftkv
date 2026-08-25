# results/

Raw output only. Nothing in this directory is hand-edited, summarized, or
retyped.

Every file is written by a committed script and carries, in its own header:

- the UTC timestamp of the run,
- the git SHA of the tree that produced it,
- the machine spec (CPU, cores, RAM, storage),
- the full flag set the binary was run with.

If a number appears in `README.md`, there is a file in here that produced it,
and `README.md` links to that file. If there is no file, there is no number.

Subdirectories:

- `bench/` — load generator output and latency histograms (Phase 8, 9)
- `failover/` — leader-kill recovery-gap distributions (Phase 8)
- `chaos/` — nemesis run logs and checker verdicts (Phase 10)
- `violations/` — archived seed + full history for any run the linearizability
  checker rejected (Phase 10). An empty directory here is a claim; a
  non-empty one is the interesting part of the project.

Currently empty: nothing has been measured yet.
