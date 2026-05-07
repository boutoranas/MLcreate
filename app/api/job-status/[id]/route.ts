import { auth } from "@clerk/nextjs/server";
import { getPool } from "@/lib/db";

export async function GET(_req: Request, { params }: { params: Promise<{ id: string }> }) {
  const { userId } = await auth();
  if (!userId) return Response.json({ error: "Unauthorized" }, { status: 401 });

  const { id } = await params;
  const pool = getPool();

  if (!pool) {
    return Response.json({ job_id: id, status: "queued", artifacts: {} });
  }

  const jobResult = await pool.query(
    `SELECT job_id, status, model_name, task_type, created_at FROM jobs WHERE job_id = $1 AND user_id = $2`,
    [id, userId]
  );

  if (jobResult.rows.length === 0) {
    return Response.json({ error: "Job not found" }, { status: 404 });
  }

  const job = jobResult.rows[0];

  const modelResult = await pool.query(
    `SELECT job_id FROM models WHERE job_id = $1`,
    [id]
  );

  const completed = modelResult.rows.length > 0;
  const status = completed ? "completed" : (job.status ?? "queued");

  return Response.json({
    job_id: id,
    status,
    artifacts: {
      model_file: completed,
    },
  });
}
