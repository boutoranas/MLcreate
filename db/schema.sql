-- PostgreSQL schema for CloudML Pipeline metadata

CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  model_name VARCHAR(255),
  task_type VARCHAR(50),
  uploader TEXT,
  status TEXT,
  user_id VARCHAR(255),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Migration for existing databases (safe to run on fresh installs too):
-- ALTER TABLE jobs ADD COLUMN IF NOT EXISTS model_name VARCHAR(255);
-- ALTER TABLE jobs ADD COLUMN IF NOT EXISTS task_type VARCHAR(50);
-- ALTER TABLE jobs ADD COLUMN IF NOT EXISTS user_id VARCHAR(255);

CREATE TABLE IF NOT EXISTS models (
  job_id TEXT PRIMARY KEY,
  model_path TEXT,
  user_id VARCHAR(255),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Migration for existing databases:
-- ALTER TABLE models ADD COLUMN IF NOT EXISTS user_id VARCHAR(255);

CREATE TABLE IF NOT EXISTS metrics (
  job_id TEXT,
  metric_name TEXT,
  metric_value DOUBLE PRECISION,
  recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
