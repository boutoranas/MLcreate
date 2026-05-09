"use client";

import { FormEvent, useState } from "react";

type PredictResponse = {
	predict_id: string;
	model_id: string;
	status: "queued" | "error";
	message?: string;
	error?: string;
};

export function PredictCSV() {
	const [selectedFile, setSelectedFile] = useState<File | null>(null);
	const [modelId, setModelId] = useState<string>("");
	const [modelType, setModelType] = useState<string>("classification");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [error, setError] = useState<string | null>(null);
	const [response, setResponse] = useState<PredictResponse | null>(null);

	async function handleSubmit(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();

		if (!selectedFile) {
			setError("Select a CSV file first.");
			return;
		}

		if (!modelId.trim()) {
			setError("Enter a model ID.");
			return;
		}

		setIsSubmitting(true);
		setError(null);
		setResponse(null);

		const formData = new FormData();
		formData.append("file", selectedFile);
		formData.append("model_id", modelId);
		formData.append("model_type", modelType);

		try {
			const predictResponse = await fetch("/api/predict", {
				method: "POST",
				body: formData,
			});

			const data = (await predictResponse.json()) as
				| PredictResponse
				| { error?: string };

			if (!predictResponse.ok) {
				const message = "error" in data ? data.error : "Prediction failed";
				throw new Error(message);
			}

			setResponse(data as PredictResponse);
		} catch (submissionError) {
			setError(
				submissionError instanceof Error
					? submissionError.message
					: "Prediction failed.",
			);
		} finally {
			setIsSubmitting(false);
		}
	}

	return (
		<section className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]">
			<form
				onSubmit={handleSubmit}
				className="rounded-[2rem] border border-slate-200 bg-white p-8 shadow-sm"
			>
				<h2 className="text-lg font-semibold text-slate-950 mb-4">
					Make Predictions
				</h2>

				<div className="space-y-4">
					<div>
						<label
							htmlFor="csv-upload-predict"
							className="block text-sm font-medium text-slate-700"
						>
							Choose CSV file
						</label>
						<input
							id="csv-upload-predict"
							name="file"
							type="file"
							accept=".csv,text/csv"
							className="mt-3 block w-full rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-700 file:mr-4 file:rounded-full file:border-0 file:bg-blue-600 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-blue-700"
							onChange={(event) =>
								setSelectedFile(event.target.files?.[0] ?? null)
							}
						/>
						<div className="mt-2 rounded-2xl bg-slate-50 px-4 py-3 text-sm text-slate-600">
							{selectedFile
								? `${selectedFile.name} (${selectedFile.size.toLocaleString()} bytes)`
								: "No file selected yet."}
						</div>
					</div>

					<div>
						<label
							htmlFor="model-id"
							className="block text-sm font-medium text-slate-700"
						>
							Model ID
						</label>
						<input
							id="model-id"
							type="text"
							placeholder="e.g., 97b9d626-09c2-479d-a02f-5fabe2bcfe42"
							value={modelId}
							onChange={(e) => setModelId(e.target.value)}
							className="mt-2 block w-full rounded-2xl border border-slate-300 bg-slate-50 px-4 py-2 text-sm text-slate-700 placeholder-slate-500 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
						/>
						<p className="mt-1 text-xs text-slate-500">
							The job ID from a completed training run
						</p>
					</div>

					<div>
						<label htmlFor="model-type-predict" className="text-sm font-medium text-slate-700">
							Model Type
						</label>
						<select
							id="model-type-predict"
							value={modelType}
							onChange={(e) => setModelType(e.target.value)}
							className="mt-2 block w-full rounded-2xl border border-slate-300 bg-slate-50 px-4 py-2 text-sm text-slate-700 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
						>
							<option value="classification">Classification</option>
							<option value="regression">Regression</option>
						</select>
					</div>

					<button
						type="submit"
						disabled={isSubmitting}
						className="w-full mt-6 inline-flex items-center justify-center rounded-full bg-blue-600 px-5 py-3 text-sm font-medium text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-blue-400"
					>
						{isSubmitting ? "Making predictions..." : "Make Predictions"}
					</button>

					{error ? (
						<p className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
							{error}
						</p>
					) : null}
				</div>
			</form>

			<div className="rounded-[2rem] border border-slate-200 bg-slate-950 p-8 text-slate-100 shadow-sm">
				<h2 className="text-lg font-semibold text-white">Prediction Results</h2>
				<p className="mt-2 text-sm leading-6 text-slate-400">
					Predictions will appear here after processing.
				</p>

				{response ? (
					<div className="mt-6 space-y-5">
						{response.status === "queued" ? (
							<>
								<dl className="grid gap-3 rounded-2xl bg-white/5 p-4 text-sm sm:grid-cols-2">
									<div>
										<dt className="text-slate-400">Predict ID</dt>
										<dd className="mt-1 text-white font-mono text-xs break-all">
											{response.predict_id}
										</dd>
									</div>
									<div>
										<dt className="text-slate-400">Model ID</dt>
										<dd className="mt-1 text-white font-mono text-xs break-all">
											{response.model_id}
										</dd>
									</div>
									<div>
										<dt className="text-slate-400">Status</dt>
										<dd className="mt-1 text-blue-400 font-medium capitalize">
											{response.status}
										</dd>
									</div>
								</dl>

								<div className="rounded-2xl bg-blue-50/10 border border-blue-200/20 p-4">
									<p className="text-sm text-blue-300 font-medium">
										⏳ Prediction is being processed by the backend consumer.
									</p>
									<p className="mt-2 text-xs text-slate-400">
										{response.message}
									</p>
									<p className="mt-3 text-xs text-slate-500">
										Check the backend logs or messages directory for results.
									</p>
								</div>
							</>
						) : (
							<p className="text-sm text-rose-400 font-medium">
								✗ Error: {response.error}
							</p>
						)}
					</div>
				) : null}
			</div>
		</section>
	);
}
