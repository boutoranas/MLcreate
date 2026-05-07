import path from "node:path";
import fs from "node:fs/promises";
import { v4 as uuidv4 } from "uuid";

export const runtime = "nodejs";

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
	modelType: string
): Promise<{ predictId: string }> {
	const predictId = uuidv4();
	const kafkaBootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";

	// Create message for predict_consumer
	const message = {
		predict_id: predictId,
		csv_path: csvPath,
		model_id: modelId,
		model_type: modelType,
		timestamp: new Date().toISOString(),
	};

	const { Kafka } = await import("kafkajs");
	const kafka = new Kafka({
		clientId: "next-predict-api",
		brokers: kafkaBootstrap.split(","),
	});
	const producer = kafka.producer();
	await producer.connect();
	await producer.send({
		topic: "predict_requested",
		messages: [{ key: predictId, value: JSON.stringify(message) }],
	});
	await producer.disconnect();

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
			return Response.json(
				{ error: "No file provided" },
				{ status: 400 }
			);
		}

		if (!modelId) {
			return Response.json(
				{ error: "No model_id provided" },
				{ status: 400 }
			);
		}

		const predictionsDir = path.join(process.cwd(), "data", "predictions");
		await fs.mkdir(predictionsDir, { recursive: true });

		// Save uploaded CSV
		const csvFileName = `predict_${Date.now()}_${file.name}`;
		const csvPath = path.join(predictionsDir, csvFileName);
		const arrayBuffer = await file.arrayBuffer();
		await fs.writeFile(csvPath, Buffer.from(arrayBuffer));

		console.log(`[Predict API] Saved CSV to ${csvPath}`);

		// Publish to Kafka for async processing
		// Convert to container-relative path for Docker consumer
		const containerCsvPath = "/workspace/data/predictions/" + csvFileName;
		const { predictId } = await publishPredictRequest(
			containerCsvPath,
			modelId,
			modelType || "classification"
		);

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
