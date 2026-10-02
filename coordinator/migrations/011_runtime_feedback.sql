ALTER TABLE tasks ADD COLUMN IF NOT EXISTS logical_partition_key TEXT;
UPDATE tasks SET logical_partition_key = partition_id::text WHERE logical_partition_key IS NULL;
ALTER TABLE tasks ALTER COLUMN logical_partition_key SET NOT NULL;

DROP INDEX IF EXISTS idx_tasks_single_partition_winner;
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_single_logical_winner
  ON tasks(job_id, logical_partition_key)
  WHERE is_winner = TRUE;

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
