"use client";

import { ChangeEvent, FormEvent, useEffect, useState } from "react";
import { PredictCSV } from "./components/PredictCSV";

type UploadResponse = {
  job_id: string;
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
    preview?: string[][] | any[];
    raw_preview?: string;
  } | null;
};

export default function Home() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<UploadResponse | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatusResponse | null>(null);
  const [taskType, setTaskType] = useState<string>('classification');

  const handleChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setTaskType(event.target.value);
  };

  useEffect(() => {
    const jobId = response?.job_id;
    if (!jobId) {
      return;
    }

    let active = true;
    const poll = async () => {
      try {
        const statusResponse = await fetch(`/api/job-status/${jobId}`);
        if (!statusResponse.ok) {
          return;
        }
        const statusData = (await statusResponse.json()) as JobStatusResponse;
        if (!active) {
          return;
        }
        setJobStatus(statusData);

        // If the status response included a parsed preview/result, merge it
        // into the upload `response` so the UI shows columns/preview.
        if (statusData.result) {
          setResponse((prev) => {
            const resultPayload = {
              row_count: statusData.result?.row_count ?? 0,
              column_count: statusData.result?.column_count ?? 0,
              columns: statusData.result?.columns ?? [],
              preview: statusData.result?.preview ?? [],
              raw_preview: statusData.result?.raw_preview ?? `Job queued: ${statusData.job_id}`,
            };

            if (prev) {
              return { ...prev, result: resultPayload };
            }

            return {
              job_id: statusData.job_id,
              status: statusData.status,
              backend: "kafka_async",
              filename: "",
              size: 0,
              result: resultPayload,
            } as UploadResponse;
          });
        }
      } catch {
        // Ignore transient polling failures and retry on next tick.
      }
    };

    poll();
    const interval = setInterval(() => {
      void poll();
    }, 2000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [response?.job_id]);

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

    try {
      const uploadResponse = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      const data = (await uploadResponse.json()) as
        | UploadResponse
        | { error?: string };

      if (!uploadResponse.ok) {
        const message = "error" in data ? data.error : undefined;
        throw new Error(message ?? "Upload failed.");
      }

      setResponse(data as UploadResponse);
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "Upload failed.",
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,_#f7fee7,_#ecfccb_35%,_#f8fafc_75%)] px-6 py-10 text-slate-900">
      <div className="mx-auto flex w-full max-w-4xl flex-col gap-8">
        <section className="rounded-[2rem] border border-lime-200/70 bg-white/90 p-8 shadow-[0_24px_80px_rgba(101,163,13,0.12)] backdrop-blur">
          <p className="text-sm font-mono uppercase tracking-[0.3em] text-lime-700">
            CSV Upload Prototype
          </p>
          <h1 className="mt-4 max-w-2xl text-4xl font-semibold tracking-tight text-slate-950">
            Send a CSV file to a dummy Python backend and render the response.
          </h1>
          <p className="mt-4 max-w-2xl text-base leading-7 text-slate-600">
            This is a minimal end-to-end flow for now: upload a CSV, post it to
            a Next route handler, pass the contents into Python, and show the
            parsed preview returned by that backend script.
          </p>
        </section>

        <section className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]">
          <form
            onSubmit={handleSubmit}
            className="rounded-[2rem] border border-slate-200 bg-white p-8 shadow-sm"
          >
            <label
              htmlFor="csv-upload"
              className="block text-sm font-medium text-slate-700"
            >
              Choose CSV file
            </label>
            <input
              id="csv-upload"
              name="file"
              type="file"
              accept=".csv,text/csv"
              className="mt-3 block w-full rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-700 file:mr-4 file:rounded-full file:border-0 file:bg-lime-600 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-lime-700"
              onChange={(event) =>
                setSelectedFile(event.target.files?.[0] ?? null)
              }
            />

            <div className="mt-4 rounded-2xl bg-slate-50 px-4 py-3 text-sm text-slate-600">
              {selectedFile
                ? `${selectedFile.name} (${selectedFile.size.toLocaleString()} bytes)`
                : "No file selected yet."}
            </div>

            <div className="flex flex-col gap-2 w-64">
              <label htmlFor="task-select" className="text-sm font-medium text-gray-700">
                Task Type
              </label>
              <select
                id="task-select"
                value={taskType}
                onChange={handleChange}
                className="block w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
              >
                <option value="classification">Classification</option>
                <option value="regression">Regression</option>
              </select>

              <p className="mt-2 text-xs text-gray-500">
                Selected mode: <span className="font-semibold text-blue-600 uppercase">{taskType}</span>
              </p>
            </div>

            <button
              type="submit"
              disabled={isSubmitting}
              className="mt-6 inline-flex items-center justify-center rounded-full bg-slate-950 px-5 py-3 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
            >
              {isSubmitting ? "Uploading..." : "Upload CSV"}
            </button>

            {error ? (
              <p className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                {error}
              </p>
            ) : null}
          </form>

          <div className="rounded-[2rem] border border-slate-200 bg-slate-950 p-8 text-slate-100 shadow-sm">
            <h2 className="text-lg font-semibold text-white">Backend response</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              The dummy Python backend returns CSV metadata and a small preview.
            </p>

            {response ? (
              <div className="mt-6 space-y-5">
                <dl className="grid gap-3 rounded-2xl bg-white/5 p-4 text-sm sm:grid-cols-2">
                  <div>
                    <dt className="text-slate-400">Job ID</dt>
                    <dd className="mt-1 text-white">{response.job_id}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400">Pipeline status</dt>
                    <dd className="mt-1 text-white">{jobStatus?.status ?? response.status}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400">Backend</dt>
                    <dd className="mt-1 text-white">{response.backend}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400">Filename</dt>
                    <dd className="mt-1 text-white">{response.filename}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400">Rows</dt>
                    <dd className="mt-1 text-white">{response.result.row_count}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400">Columns</dt>
                    <dd className="mt-1 text-white">
                      {response.result.column_count}
                    </dd>
                  </div>
                </dl>

                <div>
                  <h3 className="text-sm font-medium uppercase tracking-[0.2em] text-slate-400">
                    Column names
                  </h3>
                  <p className="mt-2 text-sm text-slate-200">
                    {response.result.columns.length > 0
                      ? response.result.columns.join(", ")
                      : "No header row detected."}
                  </p>
                </div>

                <div>
                  <h3 className="text-sm font-medium uppercase tracking-[0.2em] text-slate-400">
                    Preview
                  </h3>
                  <pre className="mt-2 overflow-x-auto rounded-2xl bg-black/30 p-4 text-xs leading-6 text-lime-200">
                    {JSON.stringify(response.result.preview, null, 2)}
                  </pre>
                </div>

                <div>
                  <h3 className="text-sm font-medium uppercase tracking-[0.2em] text-slate-400">
                    Raw snippet
                  </h3>
                  <pre className="mt-2 overflow-x-auto rounded-2xl bg-black/30 p-4 text-xs leading-6 text-slate-300">
                    {response.result.raw_preview}
                  </pre>
                </div>
              </div>
            ) : (
              <div className="mt-6 rounded-2xl border border-dashed border-white/10 bg-white/5 p-6 text-sm leading-6 text-slate-400">
                Upload a CSV to see the Python response here.
              </div>
            )}
          </div>
        </section>

        <section className="rounded-[2rem] border border-blue-200/70 bg-white/90 p-8 shadow-[0_24px_80px_rgba(37,99,235,0.12)] backdrop-blur">
          <p className="text-sm font-mono uppercase tracking-[0.3em] text-blue-700">
            Make Predictions
          </p>
          <h2 className="mt-4 max-w-2xl text-2xl font-semibold tracking-tight text-slate-950">
            Use a trained model to make predictions.
          </h2>
          <p className="mt-4 max-w-2xl text-base leading-7 text-slate-600">
            Upload a CSV file and specify a trained model ID to get predictions.
            Supports both Spark MLlib and local scikit-learn models.
          </p>
        </section>

        <PredictCSV />
      </div>
    </main>
  );
}
