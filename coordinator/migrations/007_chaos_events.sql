CREATE TABLE IF NOT EXISTS chaos_events (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
  task_id UUID,
  mode VARCHAR(40) NOT NULL,
  configuration_json JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chaos_events_job ON chaos_events(job_id, created_at);
