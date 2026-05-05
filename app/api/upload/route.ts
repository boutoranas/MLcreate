import { spawn } from "node:child_process";
import path from "node:path";

export const runtime = "nodejs";

type PythonResult = {
  row_count: number;
  column_count: number;
  columns: string[];
  preview: string[][];
  raw_preview: string;
};

function runPythonBackend(payload: string): Promise<PythonResult> {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(
      process.cwd(),
      "backend",
      "dummy_csv_backend.py",
    );
    const python = spawn("python3", [scriptPath]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    python.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    python.on("error", (error) => {
      reject(error);
    });

    python.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `Python backend exited with code ${code}.`));
        return;
      }

      try {
        resolve(JSON.parse(stdout) as PythonResult);
      } catch {
        reject(new Error("Python backend returned invalid JSON."));
      }
    });

    python.stdin.write(payload);
    python.stdin.end();
  });
}

export async function POST(request: Request) {
  const formData = await request.formData();
  const file = formData.get("file");

  if (!(file instanceof File)) {
    return Response.json({ error: "Missing CSV file." }, { status: 400 });
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    return Response.json(
      { error: "Only .csv files are supported for now." },
      { status: 400 },
    );
  }

  const text = await file.text();

  try {
    const result = await runPythonBackend(
      JSON.stringify({
        filename: file.name,
        content: text,
      }),
    );

    return Response.json({
      backend: "python3",
      filename: file.name,
      size: file.size,
      result,
    });
  } catch (error) {
    return Response.json(
      {
        error:
          error instanceof Error ? error.message : "Python backend failed.",
      },
      { status: 500 },
    );
  }
}
