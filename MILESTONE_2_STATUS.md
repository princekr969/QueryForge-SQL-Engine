# QueryForge X — Milestone 2 Current Status

**Status date:** 2 October 2026
**Milestone source:** [`QUERYFORGE_X_ROADMAP.md`](QUERYFORGE_X_ROADMAP.md)
**Overall status:** **Implementation complete; final distributed acceptance pending**

## Executive summary

Milestone 2 has been implemented across the coordinator, Node and Rust workers,
PostgreSQL metadata model, benchmark system, and React presentation interface.
All five planned phases have working source implementations and focused evidence.

The milestone is **not yet marked Complete** because the latest code has not
passed the required clean, enhanced 13-suite Docker verification matrix. The
current execution environment denies access to `/var/run/docker.sock`, so the
remaining container-level tests cannot be run from this session.

This distinction is intentional:

- **Implemented** means the required behavior and verification harness exist.
- **Focused gate passed** means the phase previously passed its targeted live
  experiment, with later fixes covered by local regressions where possible.
- **Complete** requires every focused gate plus the complete Milestone 1 and
  Milestone 2 distributed regression matrix on the current worktree.

## Status at a glance

| Phase | Implementation | Focused evidence | Final matrix | Current conclusion |
|---|---:|---:|---:|---|
| 2.5 MapReduce refinement | Complete | Passed | Pending rerun | Implemented, not finally accepted |
| 6.5 Spark-style abstractions | Complete | Passed | Pending rerun | Implemented, not finally accepted |
| 7 Query Autopsy and Cost Analyzer | Complete | Passed; local evidence strengthened | Pending rerun | Implemented, not finally accepted |
| 8 Streaming SQL | Complete | Earlier gate passed; enhanced gate pending | Pending rerun | Implemented, enhanced acceptance pending |
| 9 Educational presentation | Complete | Passed; frontend build green | Clean-deployment rerun pending | Implemented, not finally accepted |

## Current verification state

### Passing local gates

The current worktree passes the verification that does not require live
containers:

- **94/94 JavaScript tests passed** across 20 suites.
- **4/4 Rust tests passed**, covering streaming-window parity and multi-partition
  join probe selection.
- Rust formatting and locked dependency compilation pass.
- The React/Vite production build passes.
- JavaScript syntax checks pass across coordinator, worker, and benchmark code.
- `git diff --check` passes.
- Docker Compose configuration renders successfully with the `streaming` and
  `scaling` profiles.

### Last recorded focused artifact

The existing
[`benchmarks/artifacts/milestone2-report.json`](benchmarks/artifacts/milestone2-report.json)
is explicitly labeled `focused-gates-passed`. Its last recorded measurements
include:

- MapReduce combiner: **99.8839% fewer transfer bytes** for the recorded
  workload, with an identical result checksum.
- Worker cache: **130 ms cold → 88 ms warm**, with eight cache hits and an
  identical result checksum.
- Duration-aware speculation: **953 ms → 308 ms injected-straggler p99**, with
  an identical result checksum.
- Worker crash recovery: one failed attempt, replacement committed, checksum
  preserved.
- Earlier streaming recovery fixture: 600/600 accepted events, zero reported
  duplicate/lost events, exact revenue 7179.

These are retained as focused evidence, not represented as a fresh full-matrix
result for the latest worktree.

## Phase 2.5 — MapReduce refinement

**Implementation status:** Complete
**Acceptance status:** Focused gate passed; full matrix rerun pending

### Completed work

- Mapper-side aggregation is represented explicitly as `LocalCombiner` in the
  physical plan and EXPLAIN DAG.
- `AVG` transports merge-safe `(sum,count)` state rather than averaging partial
  averages.
- A controlled `combiner: false` mode emits real row-level mergeable states for
  CSV, Parquet, and join execution.
- The partition explorer validates catalog columns and bucket counts, then
  reports exact bucket rows, estimated Parquet bytes, coefficient of variation,
  skew ratio, and hot buckets.
- `POST /api/explain` reports catalog estimates and can attach measured input,
  shuffle, output-wire, CPU, elapsed, and critical-path costs.
- Measured EXPLAIN evidence is now bound to the exact SQL text and every dataset
  snapshot, preventing unrelated job evidence from being attached to a plan.
- Speculation uses observed sibling duration, launches a backup task only after
  its threshold, cancels the loser, and commits exactly one logical winner.
- The focused benchmark validates three combiner workloads, balanced and skewed
  partition fixtures, and p50/p95/p99 speculation behavior.

### Evidence locations

- `benchmarks/mapreduce_refinements.js`
- `coordinator/src/routes/explain.js`
- `coordinator/src/services/partitionExplorer.js`
- `coordinator/src/services/speculationPolicy.js`
- `worker/src/services/aggregator.js`
- `tests/aggregator.test.js`
- `tests/mapreducePolicy.test.js`
- `tests/explainEvidence.test.js`

### Remaining proof

- Rerun the MapReduce benchmark on the clean rebuilt stack.
- Confirm all three checksum invariants and transfer reductions in the new
  generated full-matrix report.
- Confirm the injected-straggler p99 improvement remains positive on the current
  machine and container limits.

## Phase 6.5 — Spark-style abstractions

**Implementation status:** Complete
**Acceptance status:** Focused gate passed; full matrix rerun pending

### Completed work

- PostgreSQL persists dataset, transformation, partition, and materialization
  lineage nodes and their dependency edges.
- Lineage metadata includes immutable snapshot checksums, partition layouts,
  logical partition keys, physical-plan information, result checksums, and
  replay metadata.
- Lazy plan creation returns a `planId` without scheduling tasks.
- `collect`, `count`, `write`, and `materialize` are explicit actions.
- Workers support bounded `MEMORY`, `DISK`, and `MEMORY_AND_DISK` cache levels.
- The cache implements LRU eviction, reference pinning, single-flight concurrent
  loads, hit/miss/eviction/bypass counters, and per-request byte budgets.
- Broadcast inputs reuse the same cached worker-local objects and release all
  references after the join.
- Accumulators are written only while committing the logical task winner.
  Failed, retried, and speculative loser attempts cannot double-count them.
- Accumulators include `rows_scanned`, `rows_passed_filter`,
  `bytes_shuffled_total`, bytes scanned, rows returned, and CPU time.
- A missing or failed derived partition can be invalidated and recomputed as one
  partition from the nearest durable lineage ancestor, with checksum comparison
  and replay-path recording.
- Rust join execution now consumes every explicit primary/probe partition rather
  than silently reading only the fallback partition.

### Evidence locations

- `coordinator/migrations/013_lineage_lazy_cache.sql`
- `coordinator/migrations/016_partition_replay_checksum.sql`
- `coordinator/src/routes/plans.js`
- `coordinator/src/routes/lineage.js`
- `worker/src/services/cacheManager.js`
- `worker-rust/src/query.rs`
- `benchmarks/spark_abstractions.js`
- `tests/cacheManager.test.js`

### Passing focused/local proof

- Earlier live proof recorded lazy planning below the 50 ms threshold.
- Earlier live proof recorded cold/warm cache reuse and checksum preservation.
- Earlier live proof recomputed one invalidated partition and preserved its
  checksum while recording an ancestor and replay path.
- Current isolated cache tests prove the byte bound, LRU eviction, pin safety,
  pressure bypass, counters, and single-flight concurrent loading.
- Current Rust tests prove explicit multi-partition join selection and fallback
  behavior.

### Remaining proof

- Rerun cold/warm tests for all three cache levels on rebuilt worker containers.
- Reconfirm eviction under a 1 MiB limit and zero leaked broadcast references.
- Kill a worker, invalidate a derived partition, and reconfirm one-partition
  lineage replay on the latest source.
- Reconfirm winner-only accumulators during an actual speculative execution.

## Phase 7 — Query Autopsy and Cost Analyzer

**Implementation status:** Complete
**Acceptance status:** Focused gate passed; full matrix rerun pending

### Completed work

- Query Autopsy identifies the critical path and ranks the top three measured
  physical operator instances by wall time.
- Each ranked operator includes scanned and produced rows, scanned bytes, wire
  bytes, CPU, peak RSS, spill, worker, attempt number, and throughput.
- Costs are attributed to storage, communication, computation, waiting, memory,
  spill, and recomputation.
- Cost domains retain their native units and are explicitly **not ranked against
  one another**, avoiding unsupported byte-versus-time comparisons.
- Suggestions include their exact trigger, such as straggler ratio, wire/scan
  ratio, spill bytes, failure count, or cardinality error.
- What-if inputs are validated and clearly labeled as model projections.
- A measured what-if executes the alternate plan and reports checksum, latency,
  transferred bytes, CPU, and peak-memory deltas.
- Named workloads persist SQL, dataset snapshots, baseline checksums, plans, and
  resource controls, then replay them against the current engine.
- The UI exposes measured domains, physical operator details, attempt timelines,
  suggestions, and the measured what-if action.

### Evidence locations

- `coordinator/src/services/queryAutopsy.js`
- `coordinator/src/services/whatIfAnalyzer.js`
- `coordinator/src/routes/query.js`
- `coordinator/src/routes/workloads.js`
- `frontend/src/components/QueryAutopsy.jsx`
- `benchmarks/cost_analyzer_replay.js`
- `tests/queryAutopsy.test.js`
- `tests/whatIfAnalyzer.test.js`

### Remaining proof

- Rerun all five canonical queries on the current build.
- Confirm each dominant-cost classification and all three measured operator
  records.
- Reconfirm that every measured what-if and all five workload replays preserve
  checksums.
- Only describe a change as an improvement when the fresh measured run shows a
  positive delta.

## Phase 8 — Streaming SQL

**Implementation status:** Complete
**Acceptance status:** Enhanced distributed gate pending

### Completed work

- Docker Compose includes a Kafka-compatible Redpanda source under the
  `streaming` profile.
- PostgreSQL persists standing queries, windows, Kafka offsets, committed
  batches, worker attribution, output epochs, accumulator deltas, and audited
  late events.
- The API supports validated `REGISTER QUERY` and `UNREGISTER QUERY` operations.
- The documented subset supports `TUMBLE`, `HOP`, and `SESSION` event-time
  windows.
- Bounded micro-batches are assigned through stream-capable workers via gRPC.
- A failed stream worker is retried on another worker before durable commit.
- Window state, source offset, accepted/audited lateness counts, accumulator
  delta, and output epoch commit in one PostgreSQL transaction.
- Kafka offsets commit afterward; replayed messages below the durable database
  offset cannot update state twice.
- Watermarks distinguish accepted late events from events routed to the durable
  late-event audit stream.
- Watermark equality now matches finalized-window semantics: an event at or
  behind the finalized watermark is audited rather than reopening state.
- Session matching no longer applies the inactivity gap twice. Events farther
  apart than the configured gap remain separate, while a bridging event can
  merge sessions correctly.
- Exact teaching-mode references are bounded and compared with live HLL and
  count-min-sketch results.
- The UI reports configured and observed HLL/CMS error.
- A repeatable-read snapshot can be materialized into an immutable Parquet
  dataset with output-epoch and source-offset lineage.
- Both Node and Rust workers implement the stream micro-batch RPC.

### Evidence locations

- `coordinator/migrations/015_streaming_sql.sql`
- `coordinator/migrations/017_stream_worker_attribution.sql`
- `coordinator/migrations/018_stream_epochs_and_evaluation.sql`
- `coordinator/src/services/streamingEngine.js`
- `coordinator/src/routes/streams.js`
- `worker/src/services/streamBatchExecutor.js`
- `worker-rust/src/main.rs`
- `benchmarks/streaming_exactly_once.js`
- `tests/streamWorker.test.js`
- `tests/streamingEngine.test.js`

### Passing focused/local proof

- Earlier live proof recorded coordinator restart convergence with no reported
  duplication or loss for the deterministic fixture.
- Local tests cover TUMBLE/HOP assignments, SESSION ownership, invalid event
  time, HLL/CMS error reporting, session-gap boundaries, and watermark equality.
- Rust tests cover HOP parity and invalid event-time rejection.

### Remaining proof

- Run the enhanced deterministic fixture for all three window types.
- Kill the coordinator once during the TUMBLE fixture and reconfirm durable
  convergence.
- Kill Worker 1 during the delayed HOP micro-batch and verify that another worker
  is recorded as the committer.
- Confirm 602 committed inputs produce 601 accepted window events and one audited
  late event, with zero duplicate/lost accepted events.
- Confirm the corrected SESSION fixture produces two sessions with counts `2,2`
  and exact revenue 17.
- Materialize the consistent epoch and compare its distributed query result with
  the exact reference computation.
- Observe the live dashboard updating approximately once per second with
  configured HLL relative error below 1%.

## Phase 9 — Educational presentation

**Implementation status:** Complete
**Acceptance status:** Focused gate passed; clean-deployment rerun pending

### Completed work

- `npm run demo:survive-crash` creates a fixture, waits until Worker 2 owns the
  selected running partition, kills it, and checks recovery and result checksum.
- The Systems Lab presents MapReduce, Spark-style lineage, and streaming as one
  connected course narrative.
- Physical operators include accessible, expandable explanations and measured
  resource details.
- The lineage view supports pointer panning, zoom controls, keyboard scrolling,
  replay status, and explicit provenance scope.
- Side-by-side ablations load only from the versioned Milestone 2 evidence
  artifact.
- Three deterministic quiz cards cover combiner behavior, lazy actions, and
  watermarks.
- The first-run walkthrough is dismissible, keyboard navigable, focus trapped,
  and supports Escape, Back, and arrow-key navigation.
- [`PRESENTATION_GUIDE.md`](PRESENTATION_GUIDE.md) provides a five-minute demo
  route and precise claims to make during evaluation.

### Evidence locations

- `benchmarks/demo_survive_crash.js`
- `frontend/src/components/LearningLab.jsx`
- `frontend/src/components/QueryAutopsy.jsx`
- `frontend/src/components/Walkthrough.jsx`
- `PRESENTATION_GUIDE.md`

### Remaining proof

- Run the scripted Worker 2 crash demo from a clean rebuilt Compose deployment
  and reconfirm completion in under 60 seconds.
- Perform the final keyboard-only walkthrough, quiz, lineage, and operator-detail
  smoke test against the rebuilt frontend.
- Capture final screenshots or a short screen recording for the course
  submission after the full artifact reports `full-matrix-passed`.

## Cross-cutting work completed

- Protobuf changes use additive field numbers and retain legacy batch fields.
- Database migrations 013–018 are additive, transactional, and idempotent.
- CSV objects and the Node/DuckDB path remain available as rollback paths.
- The Rust/DataFusion worker shares the gRPC contract and supports Parquet scan,
  aggregation, joins, and streaming micro-batches.
- Worker discovery is separated into a side-effect-free registry module.
- `/api/workers` reports live gRPC health and cache statistics without including
  dead historical registrations unless requested.
- The evidence endpoint serves the versioned Milestone 2 report to the UI.
- CI defines unit/frontend/Rust gates and the complete distributed matrix.
- Documentation avoids production-readiness and Spark-compatibility claims.

## What remains before Milestone 2 can be marked Complete

### 1. Restore Docker access

The current blocker is environmental:

```text
permission denied while trying to connect to the Docker daemon socket at
unix:///var/run/docker.sock: connect: operation not permitted
```

This is not evidence of a QueryForge test failure. It prevents the required
services and failure tests from being started or controlled.

### 2. Rebuild the clean reference deployment

```bash
cd /home/suyashagrawal/QueryForge
docker compose --profile streaming --profile scaling \
  up -d --build --wait --scale worker-scale=5
```

The expected deployment includes PostgreSQL, SeaweedFS/S3, the coordinator,
three fixed Node workers, five scaling workers, Redpanda, the frontend, and the
observability services.

### 3. Run the authoritative verification command

```bash
npm run verify
```

The matrix must pass all 13 suites:

1. DuckDB differential correctness
2. CSV/Parquet storage ablation
3. Distributed join differential
4. Approximation accuracy
5. 1/2/4/8-worker scaling
6. Adaptive-skew execution
7. MapReduce refinements
8. Spark-style abstractions
9. Cost Analyzer and workload replay
10. Streaming exactly-once behavior
11. Deterministic chaos matrix
12. Actual worker-container crash recovery
13. Coordinator restart recovery

### 4. Inspect regenerated artifacts

Required outputs under `benchmarks/artifacts/`:

- `verification-report.json`
- `milestone2-report.json`
- `README.md`
- `storage-latency.svg`
- `scaling-latency.svg`

Completion requires:

```json
{
  "verificationState": "full-matrix-passed"
}
```

The report must also contain all 13 suite results and invariant checksums.

### 5. Perform final presentation smoke checks

- Open the Systems Lab in the rebuilt frontend.
- Confirm artifact-backed ablations match the regenerated JSON.
- Complete the walkthrough and quiz using only the keyboard.
- Pan and zoom the lineage explorer.
- Inspect three ranked physical operator instances in Query Autopsy.
- Run the live stream demonstration.
- Run `npm run demo:survive-crash` and confirm it completes within 60 seconds.

### 6. Update formal status and create a repository checkpoint

After the matrix and presentation checks pass:

- Change every Milestone 2 phase in `QUERYFORGE_X_ROADMAP.md` to **Complete**.
- Replace pending language with the newly measured evidence.
- Confirm `milestone2-report.json` is the freshly generated artifact.
- Review the currently dirty worktree and create a deliberate checkpoint commit.
- Push or submit only after verifying no unrelated local changes are included.

## Current repository note

The Milestone 2 implementation is present in the working tree but has not been
consolidated into a final checkpoint commit. The worktree contains both modified
tracked files and new untracked milestone files. These changes should be
reviewed and committed only after the full matrix passes; no destructive cleanup
or reset should be used because the working tree contains the implementation.

## Final completion rule

Milestone 2 should be declared **Complete** only when all of the following are
true on the same source revision:

1. Local JavaScript, Rust, syntax, formatting, Compose-config, and frontend gates
   pass.
2. All 13 distributed suites pass from a clean rebuilt deployment.
3. Result checksums remain invariant across optimizations and recoveries.
4. `milestone2-report.json` says `full-matrix-passed`.
5. The keyboard-only presentation and Worker 2 crash demo pass on the rebuilt
   stack.
6. The roadmap is updated with the fresh measured evidence.

Until then, the precise project status is:

> **Milestone 2 is implemented and locally verified, with earlier focused live
> evidence available; final distributed acceptance is blocked only by the
> unavailable Docker execution environment.**
