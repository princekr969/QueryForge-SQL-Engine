ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS is_winner BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS job_result_rows (
  job_id      UUID REFERENCES jobs(id) ON DELETE CASCADE,
  row_index   INTEGER NOT NULL,
  row_data    JSONB NOT NULL,
  PRIMARY KEY (job_id, row_index)
);

CREATE INDEX IF NOT EXISTS idx_tasks_job_partition
  ON tasks(job_id, partition_id, attempt_number);
