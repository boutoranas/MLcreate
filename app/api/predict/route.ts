import path from "node:path";
import fs from "node:fs/promises";
import { v4 as uuidv4 } from "uuid";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

export const runtime = "nodejs";

const sqs = new SQSClient({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });
const s3 = new S3Client({ region: process.env.AWS_DEFAULT_REGION ?? "us-east-1" });

async function uploadPredictionCsvToS3(buffer: Buffer, predictId: string, filename: string): Promise<string | null> {
	const bucket = process.env.S3_BUCKET;
	if (!bucket) return null;
	const key = `predictions/${predictId}/${filename}`;
	await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: buffer, ContentType: "text/csv" }));
	return key;
}

type PredictResponse = {
	predict_id: string;
	model_id: string;
	status: "queued" | "error";
	message?: string;
	error?: string;
};

async function publishPredictRequest(
	csvPath: string,
	modelId: string,
	modelType: string,
	s3CsvKey: string | null = null,
	predictId: string = uuidv4()
): Promise<{ predictId: string }> {
	const queueUrl = process.env.SQS_QUEUE_PREDICT_REQUESTED ?? "cloudml-predict-requested";

	const message = {
		predict_id: predictId,
		csv_path: csvPath,
		s3_csv_key: s3CsvKey,
		model_id: modelId,
		model_type: modelType,
		timestamp: new Date().toISOString(),
	};

	await sqs.send(new SendMessageCommand({
		QueueUrl: queueUrl,
		MessageBody: JSON.stringify(message),
	}));

	const messagesDir = path.join(process.cwd(), "messages");
	await fs.mkdir(messagesDir, { recursive: true });
	await fs.writeFile(
		path.join(messagesDir, `predict_request_${predictId}.json`),
		JSON.stringify(message, null, 2),
		"utf8"
	);

	return { predictId };
}

export async function POST(request: Request): Promise<Response> {
	try {
		const formData = await request.formData();
		const file = formData.get("file") as File | null;
		const modelId = formData.get("model_id") as string | null;
		const modelType = formData.get("model_type") as string | null;

		if (!file) {
			return Response.json({ error: "No file provided" }, { status: 400 });
		}

		if (!modelId) {
			return Response.json({ error: "No model_id provided" }, { status: 400 });
		}

		const predictId = uuidv4();
		const csvFileName = `predict_${Date.now()}_${file.name}`;
		const arrayBuffer = await file.arrayBuffer();
		const buffer = Buffer.from(arrayBuffer);

		const s3CsvKey = await uploadPredictionCsvToS3(buffer, predictId, file.name);

		// Best-effort local write for Docker dev
		try {
			const predictionsDir = path.join(process.cwd(), "data", "predictions");
			await fs.mkdir(predictionsDir, { recursive: true });
			await fs.writeFile(path.join(predictionsDir, csvFileName), buffer);
		} catch { /* read-only on Vercel */ }

		const containerCsvPath = s3CsvKey
			? ""
			: "/workspace/data/predictions/" + csvFileName;

		const { predictId: pid } = await publishPredictRequest(
			containerCsvPath,
			modelId,
			modelType ?? "classification",
			s3CsvKey,
			predictId
		);
		void pid;

		const response: PredictResponse = {
			predict_id: predictId,
			model_id: modelId,
			status: "queued",
			message: `Prediction queued. Check /api/predict-status/${predictId} for results.`,
		};

		return Response.json(response);
	} catch (error) {
		const message = error instanceof Error ? error.message : "Unknown error";
		console.error("[Predict API] Error:", message);

		const response: PredictResponse = {
			predict_id: uuidv4(),
			model_id: "",
			status: "error",
			error: message,
		};

		return Response.json(response, { status: 500 });
	}
}
