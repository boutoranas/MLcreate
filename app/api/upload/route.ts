import path from "node:path";
import fs from "node:fs/promises";
import { v4 as uuidv4 } from "uuid";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { getPool } from "@/lib/db";

export const runtime = "nodejs";

const sqs = new SQSClient({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });

type PythonResult = {
  row_count: number;
  column_count: number;
  columns: string[];
  preview: string[][];
  raw_preview: string;
};

async function publishUploadRequest(
  csvPath: string,
  filename: string,
  taskType: string,
  modelName: string
): Promise<{ jobId: string; result: PythonResult }> {
  const jobId = uuidv4();
  const queueUrl = process.env.SQS_QUEUE_CSV_UPLOAD_REQUESTED ?? "cloudml-csv-upload-requested";
  const relativeCsvPath = path.relative(process.cwd(), csvPath).replace(/\\/g, "/");

  const message = {
    job_id: jobId,
    model_name: modelName,
    csv_path: relativeCsvPath,
    filename,
    task_type: taskType,
    model_type: taskType,
    timestamp: new Date().toISOString(),
  };

  await sqs.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(message),
  }));

  const messagesDir = path.join(process.cwd(), "messages");
  await fs.mkdir(messagesDir, { recursive: true });
  await fs.writeFile(
    path.join(messagesDir, `upload_request_${jobId}.json`),
    JSON.stringify(message, null, 2),
    "utf8"
  );

  const result: PythonResult = {
    row_count: 0,
    column_count: 0,
    columns: [],
    preview: [],
    raw_preview: `Job queued: ${jobId}`,
  };

  return { jobId, result };
}

export async function POST(request: Request) {
  const formData = await request.formData();
  const file = formData.get("file");
  const taskTypeRaw = formData.get("task_type");
  const modelNameRaw = formData.get("model_name");

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

  const content = await file.text();

  const tmpDir = path.join(process.cwd(), "data", "uploads");
  await fs.mkdir(tmpDir, { recursive: true });
  const timestamp = Date.now();
  const savePath = path.join(tmpDir, `${timestamp}_${file.name}`);
  await fs.writeFile(savePath, content, "utf8");

  try {
    const { jobId, result } = await publishUploadRequest(savePath, file.name, taskType, modelName);

    const pool = getPool();
    if (pool) {
      await pool.query(
        `INSERT INTO jobs (job_id, model_name, task_type, status, created_at)
         VALUES ($1, $2, $3, 'queued', now())
         ON CONFLICT (job_id) DO UPDATE SET status = 'queued'`,
        [jobId, modelName, taskType]
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
      result,
    });
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : "Upload processing failed." },
      { status: 500 }
    );
  }
}
