import { S3Client, HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export const runtime = "nodejs";

const s3 = new S3Client({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });

export async function GET(_req: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const bucket = process.env.S3_BUCKET;

  if (!bucket) {
    return Response.json({ predict_id: id, status: "unknown", error: "S3 not configured" }, { status: 500 });
  }

  const key = `predictions_output/${id}.csv`;

  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  } catch {
    return Response.json({ predict_id: id, status: "pending" });
  }

  const url = await getSignedUrl(
    s3,
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
      ResponseContentDisposition: `attachment; filename="predictions_${id}.csv"`,
    }),
    { expiresIn: 3600 }
  );

  return Response.json({ predict_id: id, status: "ready", download_url: url });
}
