import path from "node:path";
import fs from "node:fs/promises";

export const runtime = "nodejs";

type JobStatus = "queued" | "ingested" | "preprocessed" | "training" | "completed";

async function exists(filePath: string): Promise<boolean> {
	try {
		await fs.access(filePath);
		return true;
	} catch {
		return false;
	}
}

async function resolveJobIdByCsvPath(root: string, requestedJobId: string): Promise<string> {
	const uploadRequestPath = path.join(root, "messages", `upload_request_${requestedJobId}.json`);
	if (!(await exists(uploadRequestPath))) {
		return requestedJobId;
	}

	let requestedCsvPath = "";
	try {
		const raw = await fs.readFile(uploadRequestPath, "utf8");
		const parsed = JSON.parse(raw) as { csv_path?: string };
		requestedCsvPath = (parsed.csv_path || "").replace(/\\/g, "/");
	} catch {
		return requestedJobId;
	}

	if (!requestedCsvPath) {
		return requestedJobId;
	}

	const messagesDir = path.join(root, "messages");
	let files: string[] = [];
	try {
		files = await fs.readdir(messagesDir);
	} catch {
		return requestedJobId;
	}

	const datasetFiles = files.filter((name) => name.startsWith("dataset_uploaded_") && name.endsWith(".json"));
	for (const datasetFile of datasetFiles) {
		const filePath = path.join(messagesDir, datasetFile);
		try {
			const raw = await fs.readFile(filePath, "utf8");
			const parsed = JSON.parse(raw) as { job_id?: string; csv_path?: string };
			const candidateCsvPath = (parsed.csv_path || "").replace(/\\/g, "/");
			if (candidateCsvPath.endsWith(requestedCsvPath) || requestedCsvPath.endsWith(candidateCsvPath)) {
				return parsed.job_id || requestedJobId;
			}
		} catch {
			continue;
		}
	}

	return requestedJobId;
}

export async function GET(
	_request: Request,
	context: { params: Promise<{ jobId: string }> }
) {
	const { jobId } = await context.params;

	if (!jobId) {
		return Response.json({ error: "Missing job id." }, { status: 400 });
	}

	const root = process.cwd();
	const effectiveJobId = await resolveJobIdByCsvPath(root, jobId);
	const datasetMsg = path.join(root, "messages", `dataset_uploaded_${effectiveJobId}.json`);
	const preprocessMsg = path.join(root, "messages", `preprocess_${effectiveJobId}.json`);
	const trainMsg = path.join(root, "messages", `train_${effectiveJobId}.json`);
	const processedFile = path.join(root, "processed", `${effectiveJobId}.parquet`);
	const modelFile = path.join(root, "models", `${effectiveJobId}.pkl`);

	const [hasDatasetMsg, hasPreprocessMsg, hasTrainMsg, hasProcessedFile, hasModelFile] = await Promise.all([
		exists(datasetMsg),
		exists(preprocessMsg),
		exists(trainMsg),
		exists(processedFile),
		exists(modelFile),
	]);

	let status: JobStatus = "queued";
	if (hasTrainMsg || hasModelFile) {
		status = "completed";
	} else if (hasPreprocessMsg || hasProcessedFile) {
		status = "training";
	} else if (hasDatasetMsg) {
		status = "preprocessed";
	} else {
		status = "queued";
	}

	// If a dataset message exists, load it and surface small parsed preview info
	let result: null | {
		row_count?: number;
		column_count?: number;
		columns?: string[];
		preview?: any[];
		raw_preview?: string;
	} = null;
	if (hasDatasetMsg) {
		try {
			const raw = await fs.readFile(datasetMsg, "utf8");
			const parsed = JSON.parse(raw);
			result = {
				row_count: parsed.row_count ?? parsed.n_rows ?? undefined,
				column_count: parsed.column_count ?? (parsed.columns ? parsed.columns.length : undefined),
				columns: parsed.columns ?? undefined,
				preview: parsed.preview ?? undefined,
				raw_preview: parsed.raw_preview ?? undefined,
			};
		} catch {
			result = null;
		}
	}

	return Response.json({
		job_id: jobId,
		resolved_job_id: effectiveJobId,
		status,
		artifacts: {
			dataset_message: hasDatasetMsg,
			preprocess_message: hasPreprocessMsg,
			train_message: hasTrainMsg,
			processed_file: hasProcessedFile,
			model_file: hasModelFile,
		},
		result,
	});
}
