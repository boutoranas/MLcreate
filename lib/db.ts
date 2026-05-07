import { Pool } from 'pg';

let pool: Pool | null = null;

export function getPool(): Pool | null {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (!pool) {
    // Strip SQLAlchemy dialect suffix so pg can parse it
    // e.g. postgresql+psycopg2://... → postgresql://...
    const connectionString = url.replace(/^postgresql\+[^:]+:\/\//, 'postgresql://');
    pool = new Pool({ connectionString });
  }
  return pool;
}
