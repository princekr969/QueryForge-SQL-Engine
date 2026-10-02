CREATE TABLE IF NOT EXISTS lineage_nodes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind VARCHAR(40) NOT NULL CHECK (kind IN ('source', 'transformation', 'partition', 'materialization')),
  operator VARCHAR(80) NOT NULL,
  dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
  partition_id UUID REFERENCES partitions(id) ON DELETE CASCADE,
  job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
  logical_partition_key TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status VARCHAR(30) NOT NULL DEFAULT 'available',
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (job_id, logical_partition_key)
);

CREATE TABLE IF NOT EXISTS lineage_edges (
  parent_id UUID NOT NULL REFERENCES lineage_nodes(id) ON DELETE CASCADE,
  child_id UUID NOT NULL REFERENCES lineage_nodes(id) ON DELETE CASCADE,
  edge_type VARCHAR(40) NOT NULL DEFAULT 'depends_on',
  PRIMARY KEY (parent_id, child_id)
);

CREATE TABLE IF NOT EXISTS lazy_plans (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  sql_query TEXT NOT NULL,
  dataset_ids_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  transformations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  plan_json JSONB NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'lazy',
  last_action VARCHAR(30),
  executed_job_id UUID REFERENCES jobs(id),
  materialized_dataset_id UUID REFERENCES datasets(id),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_accumulators (
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  logical_partition_key TEXT NOT NULL,
  name VARCHAR(80) NOT NULL,
  numeric_value DOUBLE PRECISION NOT NULL,
  committed_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  committed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (job_id, logical_partition_key, name)
);

CREATE TABLE IF NOT EXISTS cache_events (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
  task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
  worker_id VARCHAR(100),
  cache_level VARCHAR(30) NOT NULL,
  cache_hit BOOLEAN NOT NULL,
  object_path TEXT NOT NULL,
  bytes BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO lineage_nodes (kind, operator, dataset_id, metadata_json)
SELECT 'source', 'DATASET_SOURCE', d.id,
       jsonb_build_object('snapshotVersion', d.snapshot_version, 'contentChecksum', d.content_checksum)
FROM datasets d
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_lineage_dataset ON lineage_nodes(dataset_id);
CREATE INDEX IF NOT EXISTS idx_lineage_job ON lineage_nodes(job_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_dataset_source
  ON lineage_nodes(dataset_id, operator) WHERE kind = 'source';
CREATE INDEX IF NOT EXISTS idx_lazy_plans_dataset ON lazy_plans(dataset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cache_events_job ON cache_events(job_id, created_at);
