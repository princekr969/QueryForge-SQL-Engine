-- QueryForge PostgreSQL Schema
-- Loaded automatically by postgres Docker image on first run

-- ── Datasets ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS datasets (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name             VARCHAR(255)  NOT NULL,
  original_filename VARCHAR(255),
  minio_path       VARCHAR(500),
  schema_json      JSONB,          -- { "columns": [{ "name": "...", "type": "..." }] }
  row_count        INTEGER,
  partition_count  INTEGER DEFAULT 3,
  storage_format   VARCHAR(20) NOT NULL DEFAULT 'csv',
  columnar_committed_at TIMESTAMP,
  content_checksum VARCHAR(64),
  snapshot_version INTEGER NOT NULL DEFAULT 1,
  generator_config_json JSONB,
  created_at       TIMESTAMP DEFAULT NOW()
);

-- ── Partitions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS partitions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id      UUID REFERENCES datasets(id) ON DELETE CASCADE,
  partition_index INTEGER NOT NULL,
  minio_path      VARCHAR(500) NOT NULL,
  parquet_path    VARCHAR(500),
  row_count       INTEGER,
  csv_byte_size   BIGINT,
  parquet_byte_size BIGINT,
  stats_json      JSONB,
  created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Workers ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workers (
  id             VARCHAR(100) PRIMARY KEY,   -- e.g. "worker-1"
  address        VARCHAR(255) NOT NULL,
  port           INTEGER      NOT NULL,
  status         VARCHAR(50)  DEFAULT 'active',  -- active | dead
  last_heartbeat TIMESTAMP,
  registered_at  TIMESTAMP DEFAULT NOW()
);

-- ── Jobs ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sql_query        TEXT    NOT NULL,
  dataset_id       UUID REFERENCES datasets(id),
  status           VARCHAR(50) DEFAULT 'pending',  -- pending | running | completed | failed
  result_row_count INTEGER,
  execution_time_ms INTEGER,
  plan_json        JSONB,
  result_checksum  VARCHAR(64),
  priority         INTEGER NOT NULL DEFAULT 0,
  resource_budget_json JSONB,
  query_context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMP DEFAULT NOW(),
  completed_at     TIMESTAMP
);

-- ── Tasks ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tasks (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id         UUID REFERENCES jobs(id) ON DELETE CASCADE,
  worker_id      VARCHAR(100) REFERENCES workers(id),
  partition_id   UUID REFERENCES partitions(id),
  logical_partition_key TEXT NOT NULL,
  status         VARCHAR(50) DEFAULT 'pending',  -- pending | running | completed | failed | reassigned
  started_at     TIMESTAMP,
  completed_at   TIMESTAMP,
  rows_processed INTEGER,
  rows_scanned   BIGINT,
  bytes_scanned  BIGINT,
  bytes_skipped  BIGINT,
  peak_memory_bytes BIGINT,
  cpu_time_micros BIGINT NOT NULL DEFAULT 0,
  transferred_bytes BIGINT NOT NULL DEFAULT 0,
  spilled_bytes BIGINT NOT NULL DEFAULT 0,
  operator_metrics_json JSONB,
  error_message  TEXT,
  attempt_number INTEGER NOT NULL DEFAULT 1,
  is_winner      BOOLEAN NOT NULL DEFAULT FALSE
);

-- Phase 1 durability: rows are committed before completion is announced, so a
-- subscriber that connects after execution receives the same result.
CREATE TABLE IF NOT EXISTS job_result_rows (
  job_id      UUID REFERENCES jobs(id) ON DELETE CASCADE,
  row_index   INTEGER NOT NULL,
  row_data    JSONB NOT NULL,
  PRIMARY KEY (job_id, row_index)
);

CREATE TABLE IF NOT EXISTS chaos_events (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
  task_id UUID,
  mode VARCHAR(40) NOT NULL,
  configuration_json JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS approximate_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  column_name VARCHAR(255) NOT NULL,
  operation VARCHAR(40) NOT NULL,
  config_json JSONB NOT NULL,
  result_json JSONB NOT NULL,
  state_bytes BIGINT NOT NULL,
  transferred_bytes BIGINT NOT NULL,
  rows_scanned BIGINT NOT NULL,
  execution_time_ms INTEGER NOT NULL,
  configured_error DOUBLE PRECISION,
  observed_error DOUBLE PRECISION,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS query_feedback (
  fingerprint VARCHAR(64) PRIMARY KEY,
  executions INTEGER NOT NULL DEFAULT 0,
  ema_result_rows DOUBLE PRECISION NOT NULL DEFAULT 0,
  ema_straggler_ratio DOUBLE PRECISION NOT NULL DEFAULT 1,
  recommended_shuffle_buckets INTEGER,
  hot_bucket_count INTEGER NOT NULL DEFAULT 0,
  last_plan_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_partitions_dataset_id ON partitions(dataset_id);
CREATE INDEX IF NOT EXISTS idx_tasks_job_id          ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status          ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_jobs_status           ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_tasks_job_partition   ON tasks(job_id, partition_id, attempt_number);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_single_logical_winner ON tasks(job_id, logical_partition_key) WHERE is_winner = TRUE;
CREATE INDEX IF NOT EXISTS idx_datasets_storage_format ON datasets(storage_format);
CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_snapshot_checksum ON datasets(content_checksum, id) WHERE content_checksum IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chaos_events_job ON chaos_events(job_id, created_at);
CREATE INDEX IF NOT EXISTS idx_approximate_runs_dataset_created ON approximate_runs(dataset_id, created_at DESC);
