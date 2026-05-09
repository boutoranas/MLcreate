import { auth } from '@clerk/nextjs/server';
import { getPool } from '@/lib/db';
import fs from 'node:fs/promises';
import path from 'node:path';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';

export const runtime = 'nodejs';

const s3 = new S3Client({ region: process.env.AWS_DEFAULT_REGION ?? 'us-east-1' });

type ModelMetadata = {
  feature_cols?: string[];
  target_column?: string | null;
};

async function loadModelMetadata(modelId: string): Promise<ModelMetadata> {
  const modelsDir = process.env.MODELS_DIR ?? path.join(process.cwd(), 'models');
  const localPath = path.join(modelsDir, `${modelId}.pkl.meta`);

  try {
    const content = await fs.readFile(localPath, 'utf8');
    return JSON.parse(content) as ModelMetadata;
  } catch {
    // Fall through to S3.
  }

  const bucket = process.env.S3_BUCKET;
  if (!bucket) return {};

  try {
    const response = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: `models/${modelId}.pkl.meta`,
    }));

    if (!response.Body || typeof response.Body.transformToString !== 'function') return {};
    const content = await response.Body.transformToString();
    return JSON.parse(content) as ModelMetadata;
  } catch {
    return {};
  }
}

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { userId } = await auth();
  if (!userId) return Response.json({ error: 'Unauthorized' }, { status: 401 });

  const { id } = await params;
  const pool = getPool();
  if (!pool) return Response.json({ error: 'Database not configured' }, { status: 503 });

  try {
    const result = await pool.query(
      `SELECT j.job_id, j.model_name, j.task_type, j.created_at, m.model_path
       FROM jobs j
       INNER JOIN models m ON j.job_id = m.job_id
       WHERE j.job_id = $1 AND j.user_id = $2`,
      [id, userId]
    );

    if (result.rows.length === 0) {
      return Response.json({ error: 'Model not found' }, { status: 404 });
    }

    const row = result.rows[0];
    const metadata = await loadModelMetadata(id);

    return Response.json({
      ...row,
      target_column: metadata.target_column ?? null,
      feature_cols: metadata.feature_cols ?? [],
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Database error';
    return Response.json({ error: message }, { status: 500 });
  }
}
