ALTER TABLE tasks ADD COLUMN IF NOT EXISTS partial_result_checksum VARCHAR(64);
