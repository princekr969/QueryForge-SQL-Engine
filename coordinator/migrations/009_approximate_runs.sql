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

CREATE INDEX IF NOT EXISTS idx_approximate_runs_dataset_created
  ON approximate_runs(dataset_id, created_at DESC);
