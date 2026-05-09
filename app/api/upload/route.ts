import path from "node:path";
import fs from "node:fs/promises";
import { v4 as uuidv4 } from "uuid";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { auth } from "@clerk/nextjs/server";
import { getPool } from "@/lib/db";

export const runtime = "nodejs";

const sqs = new SQSClient({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });
const s3 = new S3Client({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });

type PythonResult = {
  row_count: number;
  column_count: number;
  columns: string[];
  preview: string[][];
  raw_preview: string;
};

function parseCsvHeader(buffer: Buffer): string[] {
  const text = buffer.toString("utf8");
  const header: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];

    if (char === "\"") {
      if (inQuotes && text[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && text[i + 1] === "\n") i += 1;
      break;
    }

    if (!inQuotes && char === ",") {
      header.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  if (current.length > 0 || text.startsWith(",")) {
    header.push(current.trim());
  }

  return header.map((column) => column.replace(/^"|"$/g, "").trim()).filter(Boolean);
}

async function uploadToS3(buffer: Buffer, jobId: string, filename: string): Promise<string | null> {
  const bucket = process.env.S3_BUCKET;
  if (!bucket) return null;
  const key = `uploads/${jobId}/${filename}`;
  await s3.send(new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: buffer,
    ContentType: "text/csv",
  }));
  return key;
}

async function publishUploadRequest(
  localCsvPath: string,
  s3CsvKey: string | null,
  filename: string,
  jobId: string,
  taskType: string,
  modelName: string,
  targetColumn: string
): Promise<PythonResult> {
  const queueUrl = process.env.SQS_QUEUE_CSV_UPLOAD_REQUESTED ?? "cloudml-csv-upload-requested";
  const relativeCsvPath = localCsvPath
    ? path.relative(process.cwd(), localCsvPath).replace(/\\/g, "/")
    : (s3CsvKey ?? filename);

  const message = {
    job_id: jobId,
    model_name: modelName,
    csv_path: relativeCsvPath,
    s3_csv_key: s3CsvKey,
    filename,
    task_type: taskType,
    model_type: taskType,
    target_column: targetColumn,
    timestamp: new Date().toISOString(),
  };

  await sqs.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(message),
  }));

  try {
    const messagesDir = path.join(process.cwd(), "messages");
    await fs.mkdir(messagesDir, { recursive: true });
    await fs.writeFile(
      path.join(messagesDir, `upload_request_${jobId}.json`),
      JSON.stringify(message, null, 2),
      "utf8"
    );
  } catch {
    // read-only filesystem on Vercel — skip local debug write
  }

  return { row_count: 0, column_count: 0, columns: [], preview: [], raw_preview: `Job queued: ${jobId}` };
}

export async function POST(request: Request) {
  const { userId } = await auth();
  if (!userId) return Response.json({ error: "Unauthorized" }, { status: 401 });

  const formData = await request.formData();
  const file = formData.get("file");
  const taskTypeRaw = formData.get("task_type");
  const modelNameRaw = formData.get("model_name");
  const targetColumnRaw = formData.get("target_column");

  const taskType =
    typeof taskTypeRaw === "string" && taskTypeRaw.toLowerCase() === "regression"
      ? "regression"
      : "classification";

  if (!(file instanceof File)) {
    return Response.json({ error: "Missing CSV file." }, { status: 400 });
  }
  if (!file.name.toLowerCase().endsWith(".csv")) {
    return Response.json({ error: "Only .csv files are supported." }, { status: 400 });
  }

  const modelName =
    typeof modelNameRaw === "string" && modelNameRaw.trim()
      ? modelNameRaw.trim()
      : file.name.replace(/\.csv$/i, "");
  const targetColumn =
    typeof targetColumnRaw === "string" && targetColumnRaw.trim()
      ? targetColumnRaw.trim()
      : "";

  const jobId = uuidv4();
  const buffer = Buffer.from(await file.arrayBuffer());
  const csvColumns = parseCsvHeader(buffer);

  if (csvColumns.length === 0) {
    return Response.json({ error: "CSV header row is missing or could not be parsed." }, { status: 400 });
  }
  if (!targetColumn) {
    return Response.json({ error: "Missing target column." }, { status: 400 });
  }
  if (!csvColumns.includes(targetColumn)) {
    return Response.json({ error: `Target column '${targetColumn}' was not found in the CSV header.` }, { status: 400 });
  }
  if (csvColumns.filter((column) => column !== targetColumn).length === 0) {
    return Response.json({ error: "CSV must contain at least one feature column besides the target column." }, { status: 400 });
  }

  // Upload to S3 (required for cloud; skipped gracefully if S3_BUCKET not set)
  const s3CsvKey = await uploadToS3(buffer, jobId, file.name);

  // Also write locally — still needed when running in Docker with shared volumes
  let localCsvPath = "";
  try {
    const tmpDir = path.join(process.cwd(), "data", "uploads");
    await fs.mkdir(tmpDir, { recursive: true });
    localCsvPath = path.join(tmpDir, `${Date.now()}_${file.name}`);
    await fs.writeFile(localCsvPath, buffer);
  } catch {
    // Local write is best-effort when running on Vercel
  }

  try {
    const result = await publishUploadRequest(localCsvPath, s3CsvKey, file.name, jobId, taskType, modelName, targetColumn);

    const pool = getPool();
    if (pool) {
      await pool.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS target_column TEXT`);
      await pool.query(
        `INSERT INTO jobs (job_id, model_name, task_type, target_column, status, user_id, created_at)
         VALUES ($1, $2, $3, $4, 'queued', $5, now())
         ON CONFLICT (job_id) DO UPDATE SET status = 'queued', target_column = EXCLUDED.target_column`,
        [jobId, modelName, taskType, targetColumn, userId]
      );
    }

    return Response.json({
      job_id: jobId,
      model_name: modelName,
      status: "queued",
      backend: "sqs_async",
      filename: file.name,
      size: file.size,
      task_type: taskType,
      target_column: targetColumn,
      result,
    });
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : "Upload processing failed." },
      { status: 500 }
    );
  }
}
