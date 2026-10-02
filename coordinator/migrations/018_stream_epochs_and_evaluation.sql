ALTER TABLE stream_batches
  ADD COLUMN IF NOT EXISTS output_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS accepted_late_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS audited_late_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS accumulator_delta_json JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_stream_batches_epoch
  ON stream_batches(query_id, output_epoch DESC);
