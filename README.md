# QueryForge — Distributed SQL Query Engine

> A fault-tolerant distributed analytics laboratory that makes the CS404 progression from MapReduce to Spark-style lineage to event-time streaming executable, observable, and reproducible. QueryForge is a course engine—not a claim of production readiness or Spark API compatibility.
>
> **Architected after AWS Athena · Google BigQuery · Apache Drill**

---

## Verified evidence

`npm run verify` generates the authoritative checksum-validated report under
`benchmarks/artifacts/`. It covers DuckDB differential correctness, Parquet
pruning, joins and adaptive skew, approximate analytics, 1/2/4/8-worker
scaling, chaos/restarts, MapReduce ablations, lineage/cache recovery, workload
replay, and Kafka exactly-once windows. The Systems Lab reads only the generated
`milestone2-report.json` artifact for performance claims.

See [QUERYFORGE_X_ROADMAP.md](QUERYFORGE_X_ROADMAP.md) for phase evidence and
[PRESENTATION_GUIDE.md](PRESENTATION_GUIDE.md) for the five-minute demo.

## Milestone 2 course systems

- **MapReduce:** explicit merge-safe combiners, genuine combiner-off execution,
  partition-skew exploration, measured cost equations, and backup tasks.
- **Spark-style concepts:** lazy actions, durable partition lineage,
  MEMORY/DISK LRU persistence, broadcast reuse, winner-only accumulators, and
  one-partition replay.
- **Cost Analyzer:** critical path, physical-operator metrics, measured cost
  domains, checksum-safe what-if execution, and named workload replay.
- **Streaming SQL:** Kafka-compatible standing queries, worker micro-batches,
  TUMBLE/HOP/SESSION windows, watermarks, late-data audit, sketch error, and
  immutable Parquet snapshots.
- **Presentation:** artifact-backed ablations, accessible operator lessons,
  pan/zoom lineage, quiz cards, and a keyboard walkthrough.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Frontend  (React + Vite)                         │
│         Upload · SQL Editor · ⚡ Explain · Live Worker Dashboard      │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  REST + WebSocket
┌─────────────────────────▼───────────────────────────────────────────┐
│                    Coordinator  (Node.js)                             │
│                                                                       │
│  SQL Parser → Costed DAG → Adaptive Scheduler → Result Merger        │
│  ⚡ EXPLAIN · Lineage · Stream Registry · WebSocket broadcaster       │
│  CoordinatorService gRPC server  (workers register here)             │
└──────────┬──────────────────┬───────────────────┬────────────────────┘
           │ gRPC             │ gRPC              │ gRPC
    ┌──────▼──────┐   ┌───────▼──────┐   ┌───────▼──────┐
    │  Worker 1   │   │   Worker 2   │   │   Worker 3   │
    │             │   │              │   │              │
    │ ① Download  │   │ ① Download   │   │ ① Download   │
    │   partition │   │   partition  │   │   partition  │
    │ ② Filter    │   │ ② Filter     │   │ ② Filter     │
    │   (WHERE)   │   │   (WHERE)    │   │   (WHERE)    │
    │ ③ Local     │   │ ③ Local      │   │ ③ Local      │
    │   GROUP BY  │   │   GROUP BY   │   │   GROUP BY   │
    │ ④ Stream    │   │ ④ Stream     │   │ ④ Stream     │
    │   results   │   │   results    │   │   results    │
    └──────┬──────┘   └───────┬──────┘   └───────┬──────┘
           └──────────────────┼───────────────────┘
                              │ All read from
                   ┌──────────▼──────────┐
                   │ SeaweedFS (S3 API)   │
                   │ Parquet · shuffle    │
                   │ immutable snapshots │
                   └─────────────────────┘
          PostgreSQL ── metadata, jobs, tasks, workers
          Prometheus + Grafana ── metrics, dashboards
```

---

## Key Features

### ① Predicate and row-group pushdown
The legacy CSV path filters while streaming. The default Parquet path pushes
projection and predicates into DuckDB/DataFusion, pruning row groups from
catalog min/max statistics before their values reach the coordinator.

```
Worker reads CSV:
  row 1: age=22 → WHERE age > 25 → ✗ discarded (never in memory)
  row 2: age=31 → WHERE age > 25 → ✓ kept
  row 3: age=19 → WHERE age > 25 → ✗ discarded
```

### ② Partial Aggregation (MapReduce-style)
For GROUP BY queries, each worker builds a **local hash map** on its partition. Coordinator receives 3 compact maps and merges them — not millions of raw rows.

```
Worker 1 sends:  { Engineering: { count:180000, sum:14B } }   ← 8 objects
Worker 2 sends:  { Engineering: { count:181000, sum:14.1B } } ← 8 objects  
Worker 3 sends:  { Engineering: { count:180667, sum:14B } }   ← 8 objects

Coordinator merges → final AVG = total_sum / total_count
                    (NOT average of averages — mathematically correct)
```

### ③ Automatic Fault Recovery
Workers send heartbeats every 5 seconds. If a worker disappears, the coordinator
retries the logical partition on a healthy worker and atomically commits one
winner. Duration-aware speculative attempts follow the same winner rule.

### ④ EXPLAIN and measured Query Autopsy
`POST /api/explain` returns the validated plan and teaching equations. Supplying
a completed `jobId` attaches measured input, shuffle, output, CPU, and
critical-path costs. Query Autopsy ranks the physical operators and cost domains.

### ⑤ OpenTelemetry Observability
Every coordinator and worker exposes metrics on `:9464/metrics`. Custom spans on `query.plan`, `job.execute`, `task.execute` with `rows.scanned` vs `rows.passed_filter` attributes.

---

## Quick Start

```bash
git clone https://github.com/princekr969/QueryForge-
cd QueryForge-
docker compose --profile streaming up --build
```

This starts the batch engine, observability stack, and Kafka-compatible source.
Add `--profile scaling --scale worker-scale=5` for the eight-worker matrix.

| Service | URL |
|---------|-----|
| **Frontend** | http://localhost:5173 |
| **Coordinator API** | http://localhost:3000 |
| **SeaweedFS Admin** | http://localhost:9001 |
| **Prometheus** | http://localhost:9090/targets |
| **Grafana** | http://localhost:3001 (admin / admin) |

---

## SQL Support

```sql
-- Filtered scan with predicate pushdown
SELECT name, salary FROM employees WHERE salary > 60000

-- GROUP BY with multiple aggregations
SELECT department,
       COUNT(*) as total,
       AVG(salary) as avg_sal,
       MAX(salary) as max_sal,
       MIN(salary) as min_sal,
       SUM(salary) as total_sal
FROM employees
WHERE age > 25
GROUP BY department
ORDER BY total DESC

-- COUNT with filter
SELECT COUNT(*) as total FROM employees WHERE city = 'Mumbai'

-- ORDER BY + LIMIT
SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10
```

Supported aggregations: `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`

---

## Query execution — condensed flow

```
1.  POST /api/query  { sql, datasetId }
2.  node-sql-parser → AST
3.  Extract: predicates, GROUP BY, aggregations, ORDER BY, LIMIT
4.  Bind immutable catalog snapshots and physical Parquet partitions
5.  Cost local, broadcast, or hash-shuffle execution and persist the lineage DAG
6.  Dispatch bounded gRPC tasks to the least-loaded eligible workers
7.  Each worker reads the S3-compatible object with projection/row-group pruning
8.  DuckDB or DataFusion streams Arrow batches under a memory reservation
9.  A merge-safe combiner builds alias-keyed local aggregate state
10. Workers stream Arrow or aggregate states plus operator evidence
11. Coordinator commits one winner per logical partition and ignores losers
12. Coordinator: compute final AVG = total_sum / total_count
13. Coordinator: apply ORDER BY on merged result
14. Coordinator: apply LIMIT
15. Coordinator: stream rows via WebSocket → frontend
16. Frontend: render rows as they arrive
17. Coordinator atomically persists results, accumulators, lineage, and checksum
18. WebSocket: { type: 'complete', totalRows, executionTimeMs }
19. OTel spans closed with row counts
20. Prometheus metrics updated
```

---

## Fault Recovery Demo

While a query is running:

```bash
docker compose stop worker-2
```

Coordinator detects missing heartbeat → reassigns partition → query completes with 2 workers.

```bash
docker compose start worker-2   # brings it back online
```

---

## Running the verification matrix

```bash
npm ci --prefix benchmarks
npm run verify
```

This runs all 13 checksum-gated suites and writes JSON, Markdown, and SVG
artifacts to `benchmarks/artifacts/`. It requires the Compose stack with both
the `streaming` and `scaling` profiles.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/datasets/upload` | Upload CSV, returns `{ datasetId, rowCount, schema }` |
| `GET` | `/api/datasets` | List all datasets |
| `GET` | `/api/datasets/:id` | Dataset + partition details |
| `POST` | `/api/query` | Submit SQL, returns `{ jobId }` immediately |
| `GET` | `/api/query/jobs/:id` | Job status + per-task metrics |
| `POST` | `/api/explain` | Plan plus optional completed-job measured costs |
| `POST` | `/api/explain/partition` | Catalog-bound partition/skew explorer |
| `POST` | `/api/plans` | Create a lazy plan; explicit actions execute it |
| `GET` | `/api/lineage` | Dataset/operator/partition lineage DAG |
| `POST` | `/api/lineage/recover` | Replay one invalidated partition |
| `POST` | `/api/streams/register` | Register a validated standing query |
| `POST` | `/api/streams/unregister` | Execute `UNREGISTER QUERY name` |
| `POST` | `/api/workloads/:id/replay` | Replay a named workload against snapshots |
| `GET` | `/api/workers` | Live worker registry with heartbeat status |
| `GET` | `/api/health` | Coordinator health check |

**WebSocket:** `ws://localhost:3000/ws`
```json
// Subscribe
{ "type": "subscribe", "jobId": "..." }

// Receive
{ "type": "row",      "data": { "department": "Engineering", "total": 541667 } }
{ "type": "progress", "completedTasks": 2, "totalTasks": 3 }
{ "type": "complete", "totalRows": 8, "executionTimeMs": 3512 }
{ "type": "error",    "message": "..." }
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Coordinator | Node.js 24, Express 4, gRPC, WebSocket, KafkaJS |
| Workers | Node.js 24/DuckDB plus optional Rust/DataFusion, Arrow IPC, gRPC streaming |
| SQL Parsing | `node-sql-parser` (PostgreSQL dialect) |
| Object Storage | S3-compatible SeaweedFS endpoint, immutable Parquet/shuffle objects |
| Metadata | PostgreSQL 15 |
| Observability | OpenTelemetry SDK, Prometheus, Grafana |
| Containerisation | Docker Compose (9 services) |
| Frontend | React 19, Vite 8, Tailwind CSS |

---

## Project Structure

```
QueryForge/
├── coordinator/          # Coordinator node
│   ├── src/
│   │   ├── grpc/         # CoordinatorService server + WorkerService client
│   │   ├── routes/       # query, plans, lineage, streams, workloads, evidence
│   │   ├── services/     # queryPlanner, partitioner, jobManager,
│   │   │                 # adaptive joins, streaming, caches, cost analysis
│   │   ├── websocket/    # WebSocket server (ping/pong + job subscriptions)
│   │   └── db/           # PostgreSQL connection pool
│   ├── schema.sql
│   └── tracing.js        # OTel SDK (loaded before index.js via -r flag)
├── worker/               # Node/DuckDB worker
├── worker-rust/          # Rust/DataFusion worker under the same gRPC contract
│   ├── src/
│   │   ├── grpc/         # WorkerService server + CoordinatorService client
│   │   └── services/     # taskExecutor, predicateEvaluator,
│   │                     # aggregator, minioClient
│   └── tracing.js
├── frontend/             # React + Tailwind UI
│   └── src/components/   # DatasetUploader, SQLEditor, ExplainPanel,
│                         # WorkerDashboard, ResultsTable
├── proto/
│   └── dataforge.proto  # gRPC service definitions
├── monitoring/
│   ├── prometheus.yml
│   └── provisioning/     # Grafana auto-provisioned datasource + dashboard
├── benchmarks/           # 13-suite reproducible verification matrix
├── shared/               # Mergeable sketches shared by coordinator/workers
└── docker-compose.yml    # Batch, streaming, scaling, and observability profiles
```

---

## Architecture Decisions (Interview Q&A)

**Why gRPC between coordinator and workers, not REST?**
gRPC supports server-side streaming natively — workers stream partial results back as they process, without buffering everything first. REST would require workers to finish completely before sending anything, removing the streaming benefit.

**Why S3-compatible object storage and not a shared filesystem?**
A shared filesystem does not model independent distributed workers. SeaweedFS's
S3 endpoint lets every worker independently read immutable Parquet and shuffle
objects; the Rust path signs the same S3 requests directly.

**Why partial aggregation instead of sending all rows?**
For a GROUP BY query on 2M rows with 8 groups, sending raw rows means 666,667 rows per worker × 3 workers = 2M rows through the coordinator. Partial aggregation sends 8 hash map entries per worker = 24 objects total. The network difference is ~100MB vs ~200 bytes.

**What's the bottleneck right now?**
The active coordinator is still the final merge and scheduling bottleneck.
Durable PostgreSQL state makes restart/replay deterministic, but horizontal
multi-coordinator consensus is intentionally outside this course milestone.

**How would you scale beyond 3 workers?**
Partition count = worker count. Dynamic partitioning would split the dataset into N chunks at upload time based on registered workers. The current round-robin assignment already handles N workers — changing `partitionCount` from 3 to N is the only required change.

**Why are partition and worker counts independent?**
Datasets choose a validated partition count at ingestion. The scheduler assigns
those logical partitions across however many workers are currently active, so
the 1/2/4/8-worker study uses the same immutable snapshot at every scale.

---

*Built by [Prince Kumar](https://github.com/princekr969)*
