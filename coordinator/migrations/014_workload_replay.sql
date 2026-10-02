CREATE TABLE IF NOT EXISTS workloads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  description TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workload_queries (
  workload_id UUID NOT NULL REFERENCES workloads(id) ON DELETE CASCADE,
  sequence_number INTEGER NOT NULL,
  source_job_id UUID REFERENCES jobs(id),
  sql_query TEXT NOT NULL,
  dataset_id UUID NOT NULL REFERENCES datasets(id),
  dataset_ids_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  resource_budget_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  baseline_checksum VARCHAR(64),
  baseline_latency_ms INTEGER,
  baseline_plan_json JSONB,
  PRIMARY KEY (workload_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS workload_replays (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workload_id UUID NOT NULL REFERENCES workloads(id) ON DELETE CASCADE,
  overrides_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB NOT NULL,
  status VARCHAR(30) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workload_replays_workload ON workload_replays(workload_id, created_at DESC);
