CREATE TABLE IF NOT EXISTS stream_queries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL UNIQUE,
  source_topic VARCHAR(255) NOT NULL,
  statement TEXT,
  config_json JSONB NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'active',
  max_event_time TIMESTAMPTZ,
  watermark TIMESTAMPTZ,
  materialized_dataset_id UUID REFERENCES datasets(id),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stream_windows (
  query_id UUID NOT NULL REFERENCES stream_queries(id) ON DELETE CASCADE,
  window_key TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  group_key TEXT NOT NULL DEFAULT '',
  state_json JSONB NOT NULL,
  event_count BIGINT NOT NULL DEFAULT 0,
  status VARCHAR(30) NOT NULL DEFAULT 'open',
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (query_id, window_key)
);

CREATE TABLE IF NOT EXISTS stream_offsets (
  query_id UUID NOT NULL REFERENCES stream_queries(id) ON DELETE CASCADE,
  topic VARCHAR(255) NOT NULL,
  partition INTEGER NOT NULL,
  next_offset BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (query_id, topic, partition)
);

CREATE TABLE IF NOT EXISTS stream_batches (
  id BIGSERIAL PRIMARY KEY,
  query_id UUID NOT NULL REFERENCES stream_queries(id) ON DELETE CASCADE,
  topic VARCHAR(255) NOT NULL,
  partition INTEGER NOT NULL,
  first_offset BIGINT NOT NULL,
  next_offset BIGINT NOT NULL,
  event_count INTEGER NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'committed',
  committed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (query_id, topic, partition, first_offset, next_offset)
);

CREATE TABLE IF NOT EXISTS stream_late_events (
  id BIGSERIAL PRIMARY KEY,
  query_id UUID NOT NULL REFERENCES stream_queries(id) ON DELETE CASCADE,
  topic VARCHAR(255) NOT NULL,
  partition INTEGER NOT NULL,
  event_offset BIGINT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  watermark TIMESTAMPTZ NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (query_id, topic, partition, event_offset)
);

CREATE INDEX IF NOT EXISTS idx_stream_windows_query_end ON stream_windows(query_id, window_end);
CREATE INDEX IF NOT EXISTS idx_stream_batches_query ON stream_batches(query_id, committed_at DESC);
