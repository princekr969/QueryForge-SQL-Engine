CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_single_partition_winner
  ON tasks(job_id, partition_id)
  WHERE is_winner = TRUE;
