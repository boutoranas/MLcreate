import path from "node:path";
import fs from "node:fs/promises";
import { v4 as uuidv4 } from "uuid";

export const runtime = "nodejs";

type PythonResult = {
  row_count: number;
  column_count: number;
  columns: string[];
  preview: string[][];
  raw_preview: string;
};

async function publishUploadRequest(
  csvPath: string,
  filename: string
): Promise<{ jobId: string; result: PythonResult }> {
  const jobId = uuidv4();
  const kafkaBootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const relativeCsvPath = path.relative(process.cwd(), csvPath).replace(/\\/g, "/");

  // Create message for ingest_consumer
  const message = {
    job_id: jobId,
    csv_path: relativeCsvPath,
    filename: filename,
    timestamp: new Date().toISOString(),
  };

  const { Kafka } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: "next-upload-api",
    brokers: kafkaBootstrap.split(","),
  });
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: "csv_upload_requested",
    messages: [{ key: jobId, value: JSON.stringify(message) }],
  });
  await producer.disconnect();

  const messagesDir = path.join(process.cwd(), "messages");
  await fs.mkdir(messagesDir, { recursive: true });
  await fs.writeFile(
    path.join(messagesDir, `upload_request_${jobId}.json`),
    JSON.stringify(message, null, 2),
    "utf8"
  );

  // Return response indicating async processing
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

  if (!(file instanceof File)) {
    return Response.json({ error: "Missing CSV file." }, { status: 400 });
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    return Response.json(
      { error: "Only .csv files are supported." },
      { status: 400 }
    );
  }

  const content = await file.text();

  // Save CSV to disk
  const tmpDir = path.join(process.cwd(), "data", "uploads");
  await fs.mkdir(tmpDir, { recursive: true });
  const timestamp = Date.now();
  const savePath = path.join(tmpDir, `${timestamp}_${file.name}`);
  await fs.writeFile(savePath, content, "utf8");

  try {
    // Publish upload event to Kafka and return job id for status polling
    const { jobId, result } = await publishUploadRequest(savePath, file.name);

    return Response.json({
      job_id: jobId,
      status: "queued",
      backend: "kafka_async",
      filename: file.name,
      size: file.size,
      result,
    });
  } catch (error) {
    return Response.json(
      {
        error:
          error instanceof Error ? error.message : "Upload processing failed.",
      },
      { status: 500 }
    );
  }
}
