ALTER TABLE datasets ADD COLUMN IF NOT EXISTS storage_format VARCHAR(20) NOT NULL DEFAULT 'csv';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS columnar_committed_at TIMESTAMP;

ALTER TABLE partitions ADD COLUMN IF NOT EXISTS parquet_path VARCHAR(500);
ALTER TABLE partitions ADD COLUMN IF NOT EXISTS csv_byte_size BIGINT;
ALTER TABLE partitions ADD COLUMN IF NOT EXISTS parquet_byte_size BIGINT;
ALTER TABLE partitions ADD COLUMN IF NOT EXISTS stats_json JSONB;

CREATE INDEX IF NOT EXISTS idx_datasets_storage_format ON datasets(storage_format);
