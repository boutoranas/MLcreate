import { getPool } from '@/lib/db';

export const runtime = 'nodejs';

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const pool = getPool();
  if (!pool) {
    return Response.json({ error: 'Database not configured' }, { status: 503 });
  }

  try {
    const result = await pool.query(
      `SELECT j.job_id, j.model_name, j.task_type, j.created_at, m.model_path
       FROM jobs j
       INNER JOIN models m ON j.job_id = m.job_id
       WHERE j.job_id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return Response.json({ error: 'Model not found' }, { status: 404 });
    }

    return Response.json(result.rows[0]);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Database error';
    return Response.json({ error: message }, { status: 500 });
  }
}
