-- PostgreSQL schema for CloudML Pipeline metadata

CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  uploader TEXT,
  status TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS models (
  job_id TEXT PRIMARY KEY,
  model_path TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS metrics (
  job_id TEXT,
  metric_name TEXT,
  metric_value DOUBLE PRECISION,
  recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
