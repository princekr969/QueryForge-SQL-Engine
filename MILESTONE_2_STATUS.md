# QueryForge X — Milestone 2 Final Status

**Status date:** 2 October 2026

**Milestone source:** [`QUERYFORGE_X_ROADMAP.md`](QUERYFORGE_X_ROADMAP.md)

**Overall status:** **Complete and verified**

**Acceptance code revision:** `5d84e69069c52014f4cd34e36fc8d23bfb9a7f9b`

## Executive summary

Milestone 2 is implemented across the coordinator, Node and Rust workers,
PostgreSQL metadata model, Kafka-compatible streaming path, benchmark system,
and React educational interface. A clean Docker Compose deployment passed the
complete 13-suite matrix, regenerated the required evidence artifacts, and
reported `verificationState: full-matrix-passed`.

The separate presentation gates also passed. The scripted Worker 2 crash demo
preserved its checksum and completed in 2.75 seconds. A browser-level,
keyboard-driven smoke test completed the walkthrough and quiz, zoomed and
panned lineage, ran the live 120-event stream, and expanded one of three ranked
Query Autopsy operators.

## Status at a glance

| Phase | Implementation | Focused gate | Full matrix | Final status |
|---|---:|---:|---:|---|
| 2.5 MapReduce refinement | Complete | Passed | Passed | **Complete** |
| 6.5 Spark-style abstractions | Complete | Passed | Passed | **Complete** |
| 7 Query Autopsy and Cost Analyzer | Complete | Passed | Passed | **Complete** |
| 8 Streaming SQL | Complete | Passed | Passed | **Complete** |
| 9 Educational presentation | Complete | Passed | Passed | **Complete** |

## Authoritative verification

The reference stack was rebuilt from clean QueryForge Compose volumes with the
`streaming` and `scaling` profiles and five elastic workers. The local host's
port 3000 was already owned by an unrelated container, so QueryForge used its
supported `COORDINATOR_HTTP_PORT=13000` override; internal service ports and
runtime behavior were unchanged.

The authoritative command passed:

```bash
COORDINATOR_HTTP_PORT=13000 \
QUERYFORGE_URL=http://localhost:13000 \
npm run verify
```

This reference-stack run enforces the adaptive wall-clock acceptance thresholds.
GitHub Actions sets `ENFORCE_PERFORMANCE_GATES=false` because shared runners do
not provide controlled CPU capacity; CI still records those timings and enforces
all deterministic checksum, plan, skew-split, feedback, and false-positive gates.

All 13 suites passed:

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

The generated artifacts are:

- [`benchmarks/artifacts/verification-report.json`](benchmarks/artifacts/verification-report.json)
- [`benchmarks/artifacts/milestone2-report.json`](benchmarks/artifacts/milestone2-report.json)
- [`benchmarks/artifacts/README.md`](benchmarks/artifacts/README.md)
- [`benchmarks/artifacts/storage-latency.svg`](benchmarks/artifacts/storage-latency.svg)
- [`benchmarks/artifacts/scaling-latency.svg`](benchmarks/artifacts/scaling-latency.svg)

## Phase 2.5 — MapReduce refinement

**Acceptance status:** **Complete**

- Three aggregate workloads preserved identical checksums with and without the
  explicit mapper-side combiner.
- Recorded combiner transfer reductions were 99.884%, 98.914%, and 99.959%.
- The balanced partition fixture produced coefficient of variation 0.0166 and
  no hot buckets; the skewed fixture produced coefficient 1.4142 and identified
  all three populated hot buckets.
- Measured EXPLAIN exposes input, shuffle, output, CPU, and critical-path costs
  beside the teaching equation.
- Five seeded straggler runs reduced injected p99 from 967 ms to 309 ms while
  preserving the checksum.
- Speculative execution committed one logical winner and cancelled or rejected
  the losing attempt.

Primary evidence:

- `benchmarks/mapreduce_refinements.js`
- `coordinator/src/routes/explain.js`
- `coordinator/src/services/partitionExplorer.js`
- `coordinator/src/services/speculationPolicy.js`
- `worker/src/services/aggregator.js`

## Phase 6.5 — Spark-style abstractions

**Acceptance status:** **Complete**

- Twenty lazy-plan samples produced a 7 ms planning p95, below the 50 ms gate.
- `collect`, `count`, `write`, and `materialize` remain explicit actions.
- `MEMORY`, `DISK`, and `MEMORY_AND_DISK` cache runs preserved checksums and
  produced eight warm hits each.
- The 1 MiB eviction action left its selected worker at 831,882 bytes.
- Broadcast reuse covered four workers, produced eight warm hits, and leaked
  zero pinned references.
- A speculative run produced nine attempts and exactly eight committed winners;
  every accumulator reported eight committed partitions.
- Killing Worker 1 and invalidating one derived partition replayed exactly one
  partition from a recorded source ancestor and preserved its checksum.

Primary evidence:

- `coordinator/src/routes/plans.js`
- `coordinator/src/routes/lineage.js`
- `worker/src/services/cacheManager.js`
- `benchmarks/spark_abstractions.js`
- `tests/cacheManager.test.js`

## Phase 7 — Query Autopsy and Cost Analyzer

**Acceptance status:** **Complete**

- Five canonical queries produced asserted bottleneck classifications and three
  ranked physical operator records per query.
- Cost domains preserve their native units for storage, communication,
  computation, waiting, memory, spill, and recomputation.
- Suggestions retain the exact metric and threshold that triggered them.
- All five measured what-if executions preserved their result checksums.
- Three of five measured alternates improved latency; the non-improving results
  are retained without being mislabeled as improvements.
- A named five-query workload replay preserved all five checksums.
- Browser smoke verification rendered and keyboard-expanded the three ranked
  operator cards.

Primary evidence:

- `coordinator/src/services/queryAutopsy.js`
- `coordinator/src/services/whatIfAnalyzer.js`
- `coordinator/src/routes/workloads.js`
- `frontend/src/components/QueryAutopsy.jsx`
- `benchmarks/cost_analyzer_replay.js`

## Phase 8 — Streaming SQL

**Acceptance status:** **Complete**

- The deterministic Kafka-compatible fixture committed 602 input events.
- It produced 601 accepted window events, one accepted-late event, and one
  audited-late event with zero duplicate or lost accepted events.
- TUMBLE, HOP, and SESSION fixtures all passed.
- The HOP worker-failure case killed Worker 1 and committed on another worker.
- The SESSION fixture produced two sessions with counts `2,2` and exact revenue
  17.
- A coordinator SIGKILL after publish converged through durable state and
  offset replay.
- Configured HLL relative error was 0.8125%, below the 1% acceptance limit; the
  observed HLL and CMS errors were zero for the deterministic fixture.
- Materialized output contained 12 windows and exact revenue 7186, matching the
  reference computation.
- Browser smoke verification completed the live 120-event presentation stream.

Primary evidence:

- `coordinator/src/services/streamingEngine.js`
- `coordinator/src/routes/streams.js`
- `worker/src/services/streamBatchExecutor.js`
- `worker-rust/src/main.rs`
- `benchmarks/streaming_exactly_once.js`

## Phase 9 — Educational presentation

**Acceptance status:** **Complete**

The clean-deployment crash demo passed:

```text
Worker 2 killed during a delayed partition
checksum before == checksum after
failed attempts: 1
replacement/speculative attempts: 1
elapsed: 2.75 seconds
```

The keyboard-driven Chrome smoke test verified:

- all four walkthrough steps using arrow keys;
- focus entry and focus wrapping inside the modal;
- Escape dismissal;
- Systems Lab navigation;
- the `full matrix passed` evidence badge;
- an 8-node/7-edge lineage view;
- keyboard zoom from 100% to 180%;
- arrow-key horizontal lineage pan;
- all three deterministic quiz answers;
- the live 120-event stream;
- three ranked Query Autopsy operators and keyboard expansion.

The one-page presentation route remains documented in
[`PRESENTATION_GUIDE.md`](PRESENTATION_GUIDE.md).

## Cross-cutting acceptance

- JavaScript: **96/96 tests passed**.
- Rust: **4/4 tests passed** with locked dependencies.
- React/Vite production build passed.
- Rust formatting and JavaScript syntax checks passed.
- Compose configuration rendered successfully for streaming and scaling.
- Database migrations 013–018 remain additive, transactional, and idempotent.
- Protobuf additions retain legacy batch fields.
- CSV, Node/DuckDB, cache `NONE`, combiner-off, and prior-application paths
  remain available as rollback controls.
- QueryForge now honors frontend API/WebSocket overrides when the coordinator's
  host port is changed.
- Fixed course workers are deterministically scheduled before elastic replicas,
  so the Worker 2 teaching demo remains repeatable in the eight-worker stack.

## Completion conclusion

Milestone 2 satisfies every roadmap exit gate on the accepted code revision.
No required implementation or verification work remains. Screenshots or a
screen recording may still be captured as submission media, but they are
presentation packaging rather than an unmet system acceptance condition.
