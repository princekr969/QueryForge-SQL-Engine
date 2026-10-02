ALTER TABLE datasets ADD COLUMN IF NOT EXISTS content_checksum VARCHAR(64);
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS snapshot_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS generator_config_json JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_snapshot_checksum
  ON datasets(content_checksum, id)
  WHERE content_checksum IS NOT NULL;
