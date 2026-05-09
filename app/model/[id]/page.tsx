"use client";

import { FormEvent, useEffect, useState } from "react";
import { useParams } from "next/navigation";

type ModelInfo = {
  job_id: string;
  model_name: string | null;
  task_type: string | null;
  created_at: string;
  model_path: string | null;
};

type PredictResponse = {
  predict_id: string;
  model_id: string;
  status: "queued" | "error";
  message?: string;
  error?: string;
};

type PredictStatus = {
  predict_id: string;
  status: "pending" | "ready" | "unknown";
  download_url?: string;
  error?: string;
};

export default function ModelPage() {
  const params = useParams();
  const modelId = params.id as string;

  const [model, setModel] = useState<ModelInfo | null>(null);
  const [modelLoading, setModelLoading] = useState(true);
  const [modelError, setModelError] = useState<string | null>(null);

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [predictError, setPredictError] = useState<string | null>(null);
  const [predictResponse, setPredictResponse] = useState<PredictResponse | null>(null);
  const [predictStatus, setPredictStatus] = useState<PredictStatus | null>(null);

  useEffect(() => {
    if (!predictResponse || predictResponse.status !== "queued") return;
    const pid = predictResponse.predict_id;
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`/api/predict-status/${pid}`);
        const data = (await res.json()) as PredictStatus;
        setPredictStatus(data);
        if (data.status === "ready") {
          clearInterval(interval);
        }
      } catch {
        // keep polling
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [predictResponse]);

  useEffect(() => {
    fetch(`/api/models/${modelId}`)
      .then((r) => r.json())
      .then((data: ModelInfo | { error: string }) => {
        if ("error" in data) {
          setModelError(data.error);
        } else {
          setModel(data);
        }
      })
      .catch(() => setModelError("Failed to load model."))
      .finally(() => setModelLoading(false));
  }, [modelId]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!selectedFile) {
      setPredictError("Select a CSV file first.");
      return;
    }

    setIsSubmitting(true);
    setPredictError(null);
    setPredictResponse(null);

    const formData = new FormData();
    formData.append("file", selectedFile);
    formData.append("model_id", modelId);
    formData.append("model_type", model?.task_type ?? "classification");

    try {
      const res = await fetch("/api/predict", { method: "POST", body: formData });
      const data = (await res.json()) as PredictResponse | { error?: string };

      if (!res.ok) {
        const message = "error" in data ? data.error : "Prediction failed";
        throw new Error(message);
      }

      setPredictResponse(data as PredictResponse);
    } catch (err) {
      setPredictError(err instanceof Error ? err.message : "Prediction failed.");
    } finally {
      setIsSubmitting(false);
    }
  }

  if (modelLoading) {
    return (
      <main className="min-h-screen px-6 py-10">
        <p className="text-sm text-slate-400">Loading model…</p>
      </main>
    );
  }

  if (modelError) {
    return (
      <main className="min-h-screen px-6 py-10">
        <p className="text-sm text-rose-600">{modelError}</p>
      </main>
    );
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="mx-auto w-full max-w-3xl flex flex-col gap-8">
        <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
          <p className="text-xs font-mono uppercase tracking-widest text-slate-400">Model</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-900">
            {model?.model_name ?? "Unnamed Model"}
          </h1>
          <dl className="mt-3 flex flex-wrap gap-x-8 gap-y-2 text-sm">
            <div>
              <dt className="text-slate-400 inline">Task · </dt>
              <dd className="inline text-slate-700 capitalize">{model?.task_type ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-slate-400 inline">Created · </dt>
              <dd className="inline text-slate-700">
                {model?.created_at ? new Date(model.created_at).toLocaleDateString() : "—"}
              </dd>
            </div>
            <div>
              <dt className="text-slate-400 inline">ID · </dt>
              <dd className="inline text-slate-500 font-mono text-xs">{model?.job_id}</dd>
            </div>
          </dl>
        </div>

        <div className="rounded-2xl border border-slate-200 bg-white p-8 shadow-sm space-y-6">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Make predictions</h2>
            <p className="mt-1 text-sm text-slate-500">
              Upload a CSV with the same columns used during training.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label htmlFor="predict-csv" className="block text-sm font-medium text-slate-700">
                CSV file
              </label>
              <input
                id="predict-csv"
                type="file"
                accept=".csv,text/csv"
                className="mt-2 block w-full rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-5 text-sm text-slate-700 file:mr-4 file:rounded-full file:border-0 file:bg-slate-900 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-slate-700"
                onChange={(e) => setSelectedFile(e.target.files?.[0] ?? null)}
              />
              {selectedFile && (
                <p className="mt-1 text-xs text-slate-500">
                  {selectedFile.name} ({selectedFile.size.toLocaleString()} bytes)
                </p>
              )}
            </div>

            <button
              type="submit"
              disabled={isSubmitting}
              className="inline-flex items-center justify-center rounded-full bg-slate-950 px-6 py-3 text-sm font-medium text-white transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-400"
            >
              {isSubmitting ? "Sending…" : "Run Prediction"}
            </button>

            {predictError && (
              <p className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                {predictError}
              </p>
            )}
          </form>

          {predictResponse && (
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-5 space-y-3">
              <p className="text-sm font-medium text-slate-700">
                {predictResponse.status === "queued" ? "Prediction queued" : "Error"}
              </p>
              {predictResponse.status === "queued" ? (
                <>
                  <dl className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <dt className="text-slate-400 text-xs">Predict ID</dt>
                      <dd className="mt-0.5 font-mono text-xs text-slate-700 break-all">
                        {predictResponse.predict_id}
                      </dd>
                    </div>
                  </dl>
                  <p className="text-xs text-slate-500">{predictResponse.message}</p>

                  {predictStatus?.status === "ready" && predictStatus.download_url ? (
                    <a
                      href={predictStatus.download_url}
                      className="inline-flex items-center gap-2 rounded-full bg-emerald-600 px-5 py-2.5 text-sm font-medium text-white transition hover:bg-emerald-500"
                    >
                      Download predictions CSV
                    </a>
                  ) : (
                    <p className="text-xs text-slate-400 animate-pulse">
                      Waiting for results…
                    </p>
                  )}
                </>
              ) : (
                <p className="text-sm text-rose-600">{predictResponse.error}</p>
              )}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
