import { auth } from "@clerk/nextjs/server";
import { getPool } from "@/lib/db";
import { S3Client, HeadObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });

async function s3KeyExists(bucket: string, prefix: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: prefix }));
    return true;
  } catch {
    // Try as a directory prefix
    try {
      const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix + "/", MaxKeys: 1 }));
      return (res.Contents?.length ?? 0) > 0;
    } catch {
      return false;
    }
  }
}

export async function GET(_req: Request, { params }: { params: Promise<{ id: string }> }) {
  const { userId } = await auth();
  if (!userId) return Response.json({ error: "Unauthorized" }, { status: 401 });

  const { id } = await params;
  const pool = getPool();

  if (!pool) {
    return Response.json({ job_id: id, status: "queued", artifacts: {} });
  }

  const jobResult = await pool.query(
    `SELECT job_id, status FROM jobs WHERE job_id = $1 AND user_id = $2`,
    [id, userId]
  );

  if (jobResult.rows.length === 0) {
    return Response.json({ error: "Job not found" }, { status: 404 });
  }

  const modelResult = await pool.query(
    `SELECT job_id FROM models WHERE job_id = $1`,
    [id]
  );
  const completed = modelResult.rows.length > 0;

  if (completed) {
    return Response.json({ job_id: id, status: "completed", artifacts: { model_file: true } });
  }

  // Infer intermediate status from S3 artifacts
  const bucket = process.env.S3_BUCKET;
  let status = "queued";

  if (bucket) {
    const [hasModel, hasProcessed, hasUploaded] = await Promise.all([
      s3KeyExists(bucket, `models/${id}.pkl.meta`),
      s3KeyExists(bucket, `processed/${id}.parquet`),
      s3KeyExists(bucket, `uploads/${id}`),
    ]);

    if (hasModel) status = "completed";
    else if (hasProcessed) status = "training";
    else if (hasUploaded) status = "ingested";
    else status = "queued";
  }

  return Response.json({
    job_id: id,
    status,
    artifacts: { model_file: false },
  });
}
