/**
 * First-run workspace setup screen.
 * Shown after backend is ready but before the main app, if the user
 * hasn't configured their workspace directory yet.
 */

import { useState, useEffect } from "react";

interface WorkspaceSetupProps {
  onComplete: () => void;
}

export function WorkspaceSetup({ onComplete }: WorkspaceSetupProps) {
  const [workspaceDir, setWorkspaceDir] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load the default workspace path on mount
  useEffect(() => {
    (async () => {
      try {
        const { invoke } = await import("@tauri-apps/api/core");
        const defaultDir = await invoke<string>("get_default_workspace");
        setWorkspaceDir(defaultDir);
      } catch {
        setWorkspaceDir("");
      }
    })();
  }, []);

  const handleSetup = async () => {
    if (!workspaceDir.trim()) return;
    setLoading(true);
    setError(null);

    try {
      const { invoke } = await import("@tauri-apps/api/core");
      await invoke("initialize_workspace", { workspaceDir: workspaceDir.trim() });
      onComplete();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-[#0f1117]">
      <div className="flex flex-col items-center gap-8 max-w-lg text-center px-6">
        {/* Logo */}
        <div className="flex flex-col items-center gap-3">
          <img src="/forge-logo.png" alt="Forge" className="w-16 h-16" />
          <h1 className="text-3xl font-semibold text-white tracking-tight">
            Welcome to Forge
          </h1>
          <p className="text-sm text-gray-400">
            Let&apos;s set up your workspace
          </p>
        </div>

        {/* Explanation */}
        <div className="text-left w-full space-y-3">
          <p className="text-sm text-gray-300 leading-relaxed">
            Forge needs a folder to store your pipelines, outputs, and datasets.
            We&apos;ll create this structure for you:
          </p>
          <div className="bg-gray-800/50 rounded-lg px-4 py-3 font-mono text-xs text-gray-400 space-y-1">
            <div className="text-gray-300">
              Forge/
            </div>
            <div className="pl-4">pipelines/</div>
            <div className="pl-4">outputs/</div>
            <div className="pl-4">
              datasets/
              <span className="text-gray-500 ml-2">
                (sample data included)
              </span>
            </div>
          </div>
        </div>

        {/* Path input */}
        <div className="w-full text-left space-y-2">
          <label
            htmlFor="workspace-dir"
            className="text-sm font-medium text-gray-300"
          >
            Workspace folder
          </label>
          <input
            id="workspace-dir"
            type="text"
            value={workspaceDir}
            onChange={(e) => setWorkspaceDir(e.target.value)}
            autoCapitalize="off"
            autoCorrect="off"
            spellCheck={false}
            className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-gray-700 text-white text-sm
                       focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/50
                       placeholder-gray-500"
            placeholder="C:\Users\you\Documents\Forge"
          />
          <p className="text-xs text-gray-500">
            You can change this later in settings.
          </p>
        </div>

        {/* Error */}
        {error && (
          <p className="text-xs text-red-400 font-mono bg-red-950/30 rounded px-3 py-2 w-full text-left">
            {error}
          </p>
        )}

        {/* Action */}
        <button
          onClick={handleSetup}
          disabled={loading || !workspaceDir.trim()}
          className="w-full px-4 py-2.5 bg-indigo-600 hover:bg-indigo-500 disabled:bg-gray-700
                     disabled:text-gray-500 text-white text-sm font-medium rounded-lg
                     transition-colors flex items-center justify-center gap-2"
        >
          {loading ? (
            <>
              <div className="w-4 h-4 border-2 border-gray-400 border-t-white rounded-full animate-spin" />
              Setting up...
            </>
          ) : (
            "Create workspace & get started"
          )}
        </button>
      </div>
    </div>
  );
}
