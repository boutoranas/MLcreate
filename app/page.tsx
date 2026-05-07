import Link from "next/link";

export default function Home() {
  return (
    <main className="flex min-h-screen items-center justify-center px-6">
      <div className="text-center max-w-md">
        <h1 className="text-3xl font-semibold tracking-tight text-slate-900">
          Welcome to MLcreate
        </h1>
        <p className="mt-3 text-sm text-slate-500 leading-6">
          Train machine learning models on your CSV data. Select a model from the sidebar or create a new one.
        </p>
        <Link
          href="/create"
          className="mt-6 inline-flex items-center rounded-full bg-slate-950 px-6 py-3 text-sm font-medium text-white transition hover:bg-slate-700"
        >
          Create your first model
        </Link>
      </div>
    </main>
  );
}
