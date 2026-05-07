import { getPool } from '@/lib/db';

export const runtime = 'nodejs';

export async function GET() {
  const pool = getPool();
  if (!pool) return Response.json([]);

  try {
    const result = await pool.query(`
      SELECT j.job_id, j.model_name, j.task_type, j.created_at
      FROM jobs j
      INNER JOIN models m ON j.job_id = m.job_id
      ORDER BY j.created_at DESC
    `);
    return Response.json(result.rows);
  } catch {
    return Response.json([]);
  }
}
