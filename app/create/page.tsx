"use client";

import { ChangeEvent, FormEvent, useEffect, useState } from "react";
import Link from "next/link";

type UploadResponse = {
  job_id: string;
  model_name: string;
  status: string;
  backend: string;
  filename: string;
  size: number;
  result: {
    row_count: number;
    column_count: number;
    columns: string[];
    preview: string[][];
    raw_preview: string;
  };
};

type JobStatusResponse = {
  job_id: string;
  status: "queued" | "ingested" | "preprocessed" | "training" | "completed";
  artifacts: {
    dataset_message: boolean;
    preprocess_message: boolean;
    train_message: boolean;
    processed_file: boolean;
    model_file: boolean;
  };
  result?: {
    row_count?: number;
    column_count?: number;
    columns?: string[];
    preview?: string[][] | unknown[];
    raw_preview?: string;
  } | null;
};

const STATUS_STEPS = ["queued", "ingested", "preprocessed", "training", "completed"] as const;

export default function CreatePage() {
  const [modelName, setModelName] = useState("");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [taskType, setTaskType] = useState<string>("classification");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<UploadResponse | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatusResponse | null>(null);

  const handleTaskTypeChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setTaskType(event.target.value);
  };

  useEffect(() => {
    const jobId = response?.job_id;
    if (!jobId) return;

    let active = true;
    const poll = async () => {
      try {
        const statusResponse = await fetch(`/api/job-status/${jobId}`);
        if (!statusResponse.ok) return;
        const statusData = (await statusResponse.json()) as JobStatusResponse;
        if (!active) return;
        setJobStatus(statusData);

        if (statusData.result) {
          setResponse((prev) => {
            const resultPayload = {
              row_count: statusData.result?.row_count ?? 0,
              column_count: statusData.result?.column_count ?? 0,
              columns: statusData.result?.columns ?? [],
              preview: (statusData.result?.preview as string[][]) ?? [],
              raw_preview: statusData.result?.raw_preview ?? `Job queued: ${statusData.job_id}`,
            };
            if (prev) return { ...prev, result: resultPayload };
            return {
              job_id: statusData.job_id,
              model_name: "",
              status: statusData.status,
              backend: "sqs_async",
              filename: "",
              size: 0,
              result: resultPayload,
            } as UploadResponse;
          });
        }
      } catch {
        // Ignore transient polling failures.
      }
    };

    if (jobStatus?.status === "completed") return;

    poll();
    const interval = setInterval(() => void poll(), 2000);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [response?.job_id, jobStatus?.status]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!selectedFile) {
      setError("Select a CSV file first.");
      return;
    }

    setIsSubmitting(true);
    setError(null);
    setResponse(null);
    setJobStatus(null);

    const formData = new FormData();
    formData.append("file", selectedFile);
    formData.append("task_type", taskType);
    formData.append("model_name", modelName.trim() || selectedFile.name.replace(/\.csv$/i, ""));

    try {
      const uploadResponse = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      const data = (await uploadResponse.json()) as UploadResponse | { error?: string };

      if (!uploadResponse.ok) {
        const message = "error" in data ? data.error : undefined;
        throw new Error(message ?? "Upload failed.");
      }

      setResponse(data as UploadResponse);
    } catch (submissionError) {
      setError(
        submissionError instanceof Error ? submissionError.message : "Upload failed."
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  const currentStepIndex = jobStatus
    ? STATUS_STEPS.indexOf(jobStatus.status)
    : -1;

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="mx-auto w-full max-w-3xl flex flex-col gap-8">
        <div>
          <p className="text-xs font-mono uppercase tracking-widest text-slate-400">New Model</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-900">
            Train a model on your CSV
          </h1>
          <p className="mt-2 text-sm text-slate-500">
            Upload a labelled CSV file. The pipeline will preprocess, train, and store your model.
          </p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="rounded-2xl border border-slate-200 bg-white p-8 shadow-sm space-y-6"
        >
          <div>
            <label htmlFor="model-name" className="block text-sm font-medium text-slate-700">
              Model name
            </label>
            <input
              id="model-name"
              type="text"
              placeholder="e.g., Customer Churn Classifier"
              value={modelName}
              onChange={(e) => setModelName(e.target.value)}
              className="mt-2 block w-full rounded-xl border border-slate-300 bg-slate-50 px-4 py-2.5 text-sm text-slate-900 placeholder-slate-400 focus:border-slate-500 focus:outline-none focus:ring-1 focus:ring-slate-500"
            />
          </div>

          <div>
            <label htmlFor="csv-upload" className="block text-sm font-medium text-slate-700">
              Training CSV
            </label>
            <input
              id="csv-upload"
              name="file"
              type="file"
              accept=".csv,text/csv"
              className="mt-2 block w-full rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-5 text-sm text-slate-700 file:mr-4 file:rounded-full file:border-0 file:bg-slate-900 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-slate-700"
              onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
            />
            {selectedFile && (
              <p className="mt-1 text-xs text-slate-500">
                {selectedFile.name} ({selectedFile.size.toLocaleString()} bytes)
              </p>
            )}
          </div>

          <div className="w-64">
            <label htmlFor="task-select" className="block text-sm font-medium text-slate-700">
              Task type
            </label>
            <select
              id="task-select"
              value={taskType}
              onChange={handleTaskTypeChange}
              className="mt-2 block w-full rounded-xl border border-slate-300 bg-slate-50 px-3 py-2.5 text-sm text-slate-900 focus:border-slate-500 focus:outline-none focus:ring-1 focus:ring-slate-500"
            >
              <option value="classification">Classification</option>
              <option value="regression">Regression</option>
            </select>
          </div>

          <button
            type="submit"
            disabled={isSubmitting}
            className="inline-flex items-center justify-center rounded-full bg-slate-950 px-6 py-3 text-sm font-medium text-white transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            {isSubmitting ? "Starting…" : "Start Training"}
          </button>

          {error && (
            <p className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
              {error}
            </p>
          )}
        </form>

        {response && (
          <div className="rounded-2xl border border-slate-200 bg-white p-8 shadow-sm space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-slate-900">
                  {response.model_name || "Model"}
                </h2>
                <p className="mt-0.5 text-xs text-slate-500 font-mono">{response.job_id}</p>
              </div>
              {jobStatus?.status === "completed" && (
                <Link
                  href={`/model/${response.job_id}`}
                  className="inline-flex items-center rounded-full bg-slate-950 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 transition"
                >
                  Open model →
                </Link>
              )}
            </div>

            <div className="space-y-2">
              <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">
                Pipeline status
              </p>
              <div className="flex items-center gap-2 flex-wrap">
                {STATUS_STEPS.map((step, i) => (
                  <div key={step} className="flex items-center gap-2">
                    <span
                      className={`rounded-full px-3 py-1 text-xs font-medium capitalize ${
                        i < currentStepIndex
                          ? "bg-green-100 text-green-700"
                          : i === currentStepIndex
                          ? "bg-slate-950 text-white"
                          : "bg-slate-100 text-slate-400"
                      }`}
                    >
                      {step}
                    </span>
                    {i < STATUS_STEPS.length - 1 && (
                      <span className="text-slate-300 text-xs">›</span>
                    )}
                  </div>
                ))}
              </div>
            </div>

            {response.result.columns.length > 0 && (
              <div>
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-400 mb-1">
                  Columns detected
                </p>
                <p className="text-sm text-slate-700">{response.result.columns.join(", ")}</p>
              </div>
            )}

            {response.result.raw_preview && (
              <div>
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-400 mb-1">
                  Preview
                </p>
                <pre className="overflow-x-auto rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-300">
                  {response.result.raw_preview}
                </pre>
              </div>
            )}
          </div>
        )}
      </div>
    </main>
  );
}
