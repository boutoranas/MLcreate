"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { UserButton, SignOutButton, useUser, useAuth } from "@clerk/nextjs";

type Model = {
  job_id: string;
  model_name: string | null;
  task_type: string | null;
  created_at: string;
};

export function Sidebar() {
  const { isSignedIn } = useAuth();
  const { user } = useUser();
  const [models, setModels] = useState<Model[]>([]);
  const pathname = usePathname();

  useEffect(() => {
    if (!isSignedIn) return;
    fetch("/api/models")
      .then((r) => r.json())
      .then((data) => { if (Array.isArray(data)) setModels(data); })
      .catch(() => {});
  }, [pathname, isSignedIn]);

  useEffect(() => {
    if (!isSignedIn) return;
    const interval = setInterval(() => {
      fetch("/api/models")
        .then((r) => r.json())
        .then((data) => { if (Array.isArray(data)) setModels(data); })
        .catch(() => {});
    }, 15000);
    return () => clearInterval(interval);
  }, [isSignedIn]);

  if (!isSignedIn) return null;

  return (
    <aside className="w-56 shrink-0 h-screen bg-slate-950 text-white flex flex-col border-r border-slate-800 overflow-hidden">
      <div className="px-4 py-5 border-b border-slate-800">
        <span className="text-sm font-semibold tracking-tight text-white">MLcreate</span>
      </div>

      <div className="px-3 pt-3 pb-1">
        <Link
          href="/create"
          className={`flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
            pathname === "/create"
              ? "bg-white/15 text-white"
              : "text-slate-300 hover:bg-white/8 hover:text-white"
          }`}
        >
          <span className="text-lg leading-none">+</span>
          New Model
        </Link>
      </div>

      {models.length > 0 && (
        <>
          <div className="px-4 pt-3 pb-1 text-[10px] font-semibold uppercase tracking-widest text-slate-500">
            Your Models
          </div>
          <nav className="flex-1 overflow-y-auto px-3 pb-4 space-y-0.5">
            {models.map((model) => {
              const active = pathname === `/model/${model.job_id}`;
              return (
                <Link
                  key={model.job_id}
                  href={`/model/${model.job_id}`}
                  className={`block rounded-lg px-3 py-2 transition-colors ${
                    active ? "bg-white/15 text-white" : "text-slate-300 hover:bg-white/8 hover:text-white"
                  }`}
                >
                  <div className="text-sm font-medium truncate">
                    {model.model_name ?? "Unnamed"}
                  </div>
                  <div className="mt-0.5 text-[11px] text-slate-400 truncate">
                    {model.task_type ?? "—"} &middot;{" "}
                    {new Date(model.created_at).toLocaleDateString()}
                  </div>
                </Link>
              );
            })}
          </nav>
        </>
      )}

      <div className="mt-auto px-3 pb-3 pt-3 border-t border-slate-800 space-y-4">
        <div className="flex items-center gap-3 px-1">
          <UserButton appearance={{ elements: { avatarBox: "w-7 h-7" } }} />
          <span className="text-sm text-slate-300 truncate">
            {user?.username ?? user?.emailAddresses[0]?.emailAddress ?? ""}
          </span>
        </div>
        <SignOutButton>
          <button className="w-full flex items-center rounded-lg px-3 py-2 text-sm font-medium text-red-400 bg-red-950/50 hover:bg-red-900/60 transition-colors">
            Log out
          </button>
        </SignOutButton>
      </div>
    </aside>
  );
}
