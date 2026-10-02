# QueryForge X — five-minute presentation guide

## The one-sentence pitch

QueryForge X is a from-scratch distributed SQL laboratory that makes the
MapReduce → Spark lineage → streaming progression observable and experimentally
testable, while preserving SQL results with checksums and single-winner commits.

## The five-minute route

1. **00:00–00:45 — Architecture.** Open **Systems Lab** and point across the
   three execution-model cards. The same Parquet catalog feeds bounded batch,
   replayable lazy plans, and Kafka standing queries.
2. **00:45–01:45 — MapReduce proof.** Run a grouped aggregate. In Query Autopsy,
   show scan bytes, wire bytes, CPU, RSS, critical path, and the result checksum.
   Explain that `AVG` moves `(sum,count)`, never averages partial averages.
3. **01:45–02:30 — Communication ablation.** Cite the artifact-backed combiner
   trial: the three seeded workloads preserve checksums while materially reducing
   worker-to-coordinator bytes. The partition explorer reports `hash(column) mod R`,
   CV, skew ratio, and hot buckets.
4. **02:30–03:20 — Spark ideas.** Create a lazy plan: no tasks exist until
   `collect`, `count`, `write`, or `materialize`. Open the lineage graph, then
   mention bounded MEMORY/DISK LRU caches, one-winner accumulators, broadcast
   reuse, and one-partition replay.
5. **03:20–04:10 — Streams.** Run the 120-event live trial. Point to Kafka
   committed events, TUMBLE windows, the watermark, HLL’s configured <1% error,
   late-event audit, and stream-to-Parquet materialization.
6. **04:10–05:00 — Failure moment.** Run `npm run demo:survive-crash`. Worker 2
   is killed during a synchronized slow partition; another attempt wins and the
   before/after SHA-256 checksums match.

## Claims to make precisely

- “Spark-style educational primitives,” not Spark API compatibility.
- Partition/operator lineage, not full cell-level provenance.
- A what-if projection is labeled as a model; the UI’s alternate-plan button
  also performs a measured run and checks the checksum.
- PostgreSQL commits stream window state and the next Kafka offset atomically.
  Kafka commits afterward, so a crash may replay an uncommitted batch but cannot
  duplicate a committed event in state.
- HLL’s configured relative standard error is `1.04 / sqrt(2^14) = 0.8125%`.

## Reproducible commands

```bash
npm test
npm run demo:survive-crash
npm run demo:streaming
npm run verify
```

The complete verification matrix emits JSON and SVG evidence under
`benchmarks/artifacts/`.
