import { useEffect, useState } from "react";

function detectTauri(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

interface SettingsModalProps {
  open: boolean;
  onClose: () => void;
}

interface WorkspaceConfig {
  workspace_dir: string;
  setup_complete: boolean;
}

export function SettingsModal({ open, onClose }: SettingsModalProps) {
  const isTauri = detectTauri();
  const [config, setConfig] = useState<WorkspaceConfig | null>(null);
  const [workspaceDir, setWorkspaceDir] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);

  // Load settings when the modal opens
  useEffect(() => {
    if (!open) return;
    setLoading(true);
    setSaveMsg(null);

    if (isTauri) {
      import("@tauri-apps/api/core").then(({ invoke }) => {
        invoke<WorkspaceConfig>("load_settings")
          .then((cfg) => {
            setConfig(cfg);
            setWorkspaceDir(cfg.workspace_dir);
          })
          .catch(() => {
            setConfig(null);
            setWorkspaceDir("");
          })
          .finally(() => setLoading(false));
      });
    } else {
      // In browser mode, nothing to load
      setLoading(false);
    }
  }, [open, isTauri]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  if (!open) return null;

  const handleBrowse = async () => {
    try {
      const { open: openDialog } = await import(
        "@tauri-apps/plugin-dialog"
      );
      const selected = await openDialog({
        directory: true,
        multiple: false,
        title: "Select Workspace Directory",
        defaultPath: workspaceDir || undefined,
      });
      if (selected && typeof selected === "string") {
        setWorkspaceDir(selected);
      }
    } catch (err) {
      console.error("Failed to open folder dialog:", err);
    }
  };

  const handleSave = async () => {
    if (!isTauri) return;
    setSaving(true);
    setSaveMsg(null);
    try {
      const { invoke } = await import("@tauri-apps/api/core");
      await invoke("save_settings", {
        config: {
          workspace_dir: workspaceDir,
          setup_complete: true,
        },
      });
      setSaveMsg("Settings saved");
      setTimeout(() => setSaveMsg(null), 3000);
    } catch (err) {
      setSaveMsg(
        "Save failed: " + (err instanceof Error ? err.message : String(err)),
      );
    } finally {
      setSaving(false);
    }
  };

  const handleCancel = () => {
    // Reset to original values
    if (config) {
      setWorkspaceDir(config.workspace_dir);
    }
    setSaveMsg(null);
    onClose();
  };

  const hasChanges = config ? workspaceDir !== config.workspace_dir : false;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Settings"
      className="fixed inset-0 z-40 flex items-start justify-center pt-20 bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(e) => {
        if (e.target === e.currentTarget) handleCancel();
      }}
    >
      <div className="w-full max-w-lg bg-forge-surface border border-forge-border rounded-lg shadow-2xl overflow-hidden animate-fade-in-scale">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-forge-border">
          <h2 className="text-forge-text font-semibold text-sm">Settings</h2>
          <button
            onClick={handleCancel}
            aria-label="Close"
            className="text-forge-muted hover:text-forge-text transition-colors"
          >
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path
                d="M1 1l12 12M13 1L1 13"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="px-5 py-5 space-y-5">
          {loading ? (
            <p className="text-forge-muted text-sm text-center py-8 animate-pulse">
              Loading settings...
            </p>
          ) : !isTauri ? (
            <p className="text-forge-muted text-sm text-center py-8">
              Settings are only available in the desktop app.
            </p>
          ) : (
            <>
              {/* Workspace Directory */}
              <div className="space-y-2">
                <label className="block text-sm font-medium text-forge-text">
                  Workspace Directory
                </label>
                <p className="text-xs text-forge-muted">
                  Where Forge stores pipelines, datasets, and outputs.
                </p>
                <div className="flex items-center gap-2">
                  <input
                    type="text"
                    value={workspaceDir}
                    onChange={(e) => setWorkspaceDir(e.target.value)}
                    className="flex-1 rounded border border-forge-border bg-forge-bg px-3 py-2 text-sm text-forge-text focus:border-forge-accent focus:outline-none font-mono"
                    placeholder="/path/to/workspace"
                  />
                  <button
                    onClick={() => { void handleBrowse(); }}
                    className="px-3 py-2 rounded text-sm text-forge-text bg-forge-border/40 hover:bg-forge-border/60 transition-colors flex-shrink-0"
                  >
                    Browse
                  </button>
                </div>
              </div>

              {/* Info section */}
              <div className="rounded border border-forge-border/50 bg-forge-bg/50 px-4 py-3 space-y-1">
                <p className="text-xs text-forge-muted">
                  <span className="text-forge-text font-medium">Pipelines:</span>{" "}
                  {workspaceDir ? `${workspaceDir}/pipelines` : "---"}
                </p>
                <p className="text-xs text-forge-muted">
                  <span className="text-forge-text font-medium">Datasets:</span>{" "}
                  {workspaceDir ? `${workspaceDir}/datasets` : "---"}
                </p>
                <p className="text-xs text-forge-muted">
                  <span className="text-forge-text font-medium">Outputs:</span>{" "}
                  {workspaceDir ? `${workspaceDir}/outputs` : "---"}
                </p>
              </div>
            </>
          )}
        </div>

        {/* Footer */}
        {!loading && isTauri && (
          <div className="flex items-center justify-between border-t border-forge-border px-5 py-3">
            <div className="min-h-[20px]">
              {saveMsg && (
                <span
                  className={`text-xs animate-fade-in-up ${
                    saveMsg.startsWith("Save failed")
                      ? "text-forge-error"
                      : "text-forge-complete"
                  }`}
                >
                  {saveMsg.startsWith("Settings saved") ? "\u2713 " : ""}
                  {saveMsg}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <button onClick={handleCancel} className="btn-ghost">
                Cancel
              </button>
              <button
                onClick={() => { void handleSave(); }}
                disabled={saving || !hasChanges}
                className="px-4 py-1.5 rounded text-sm font-semibold text-white bg-forge-accent hover:bg-forge-accent-hover transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {saving ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
