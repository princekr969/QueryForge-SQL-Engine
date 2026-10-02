ALTER TABLE stream_batches
  ADD COLUMN IF NOT EXISTS worker_id VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_stream_batches_worker
  ON stream_batches(worker_id, committed_at DESC);
