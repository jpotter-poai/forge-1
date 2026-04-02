import { useEffect } from "react";
import type { CustomBlockEntry } from "@/api/client";

interface PluginManagerModalProps {
  open: boolean;
  plugins: CustomBlockEntry[];
  loading: boolean;
  error: string | null;
  onInstall: () => void;
  onDownloadTemplate: () => void;
  onRefresh: () => void;
  onClose: () => void;
  onExport: (plugin: CustomBlockEntry) => void;
  onDelete: (plugin: CustomBlockEntry) => void;
}

export function PluginManagerModal({
  open,
  plugins,
  loading,
  error,
  onInstall,
  onDownloadTemplate,
  onRefresh,
  onClose,
  onExport,
  onDelete,
}: PluginManagerModalProps) {
  useEffect(() => {
    if (!open) return;
    const handler = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose, open]);

  if (!open) {
    return null;
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Manage Plugins"
      className="fixed inset-0 z-40 flex items-start justify-center pt-16 bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div className="w-full max-w-4xl mx-4 bg-forge-surface border border-forge-border rounded-lg shadow-2xl overflow-hidden animate-fade-in-scale">
        <div className="flex items-center justify-between px-5 py-4 border-b border-forge-border">
          <div>
            <h2 className="text-forge-text font-semibold text-sm">Manage Plugins</h2>
            <p className="text-[11px] text-forge-muted mt-0.5">
              Installed plugin files and the blocks they provide.
            </p>
          </div>
          <div className="flex items-center gap-2 flex-wrap justify-end">
            <button onClick={onInstall} className="btn-ghost text-xs">
              Import Plugin
            </button>
            <button onClick={onDownloadTemplate} className="btn-ghost text-xs">
              Download Template
            </button>
            <button onClick={onRefresh} className="btn-ghost text-xs">
              Refresh
            </button>
            <button
              onClick={onClose}
              aria-label="Close"
              className="text-forge-muted hover:text-forge-text transition-colors"
            >
              ✕
            </button>
          </div>
        </div>

        <div className="max-h-[72vh] overflow-y-auto">
          {loading ? (
            <p className="text-forge-muted text-xs text-center py-10 animate-pulse">
              Loading plugins…
            </p>
          ) : error ? (
            <div className="px-5 py-8 text-center">
              <p className="text-sm text-forge-error">{error}</p>
              <button onClick={onRefresh} className="btn-ghost mt-3 text-xs">
                Retry
              </button>
            </div>
          ) : plugins.length === 0 ? (
            <div className="px-5 py-10 text-center space-y-2">
              <p className="text-sm text-forge-text">No plugins installed.</p>
              <p className="text-xs text-forge-muted">
                Install a `.py` plugin file from the Plugins menu or drag one onto the canvas.
              </p>
              <div className="flex items-center justify-center gap-2 pt-2">
                <button onClick={onInstall} className="btn-ghost text-xs">
                  Import Plugin
                </button>
                <button onClick={onDownloadTemplate} className="btn-ghost text-xs">
                  Download Template
                </button>
              </div>
            </div>
          ) : (
            <ul className="divide-y divide-forge-border">
              {plugins.map((plugin) => {
                const pluginBlocks = Array.isArray(plugin.blocks) ? plugin.blocks : [];
                const requirements = Array.isArray(plugin.requirements)
                  ? plugin.requirements
                  : [];

                return (
                <li key={plugin.filename} className="px-5 py-4">
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <h3 className="text-sm font-semibold text-forge-text">
                          {plugin.title}
                        </h3>
                        <span className="text-[11px] font-mono text-forge-muted">
                          {plugin.filename}
                        </span>
                      </div>
                      <p className="text-xs text-forge-muted mt-1 leading-relaxed">
                        {plugin.description}
                      </p>

                      <div className="mt-3 space-y-2">
                        <div>
                          <p className="text-[10px] font-medium uppercase tracking-wider text-forge-muted mb-1">
                            Blocks
                          </p>
                          {pluginBlocks.length > 0 ? (
                            <div className="flex flex-wrap gap-1.5">
                              {pluginBlocks.map((block) => (
                                <span
                                  key={`${plugin.filename}:${block.key}`}
                                  className="inline-flex items-center gap-1 rounded border border-forge-border bg-forge-bg px-2 py-1 text-[11px] text-forge-text"
                                  title={`${block.category} · v${block.version}`}
                                >
                                  <span>{block.name}</span>
                                  <span className="text-forge-muted">
                                    {block.category}
                                  </span>
                                </span>
                              ))}
                            </div>
                          ) : (
                            <p className="text-xs text-forge-muted">
                              No blocks are currently registered from this file.
                            </p>
                          )}
                        </div>

                        {requirements.length > 0 && (
                          <div>
                            <p className="text-[10px] font-medium uppercase tracking-wider text-forge-muted mb-1">
                              Requirements
                            </p>
                            <div className="flex flex-wrap gap-1.5">
                              {requirements.map((requirement) => (
                                <span
                                  key={`${plugin.filename}:${requirement}`}
                                  className="inline-flex items-center rounded border border-forge-border bg-forge-bg px-2 py-1 text-[11px] font-mono text-forge-muted"
                                >
                                  {requirement}
                                </span>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0">
                      <button
                        onClick={() => onExport(plugin)}
                        className="btn-ghost text-xs"
                        title="Download the installed plugin source file"
                      >
                        Download
                      </button>
                      <button
                        onClick={() => onDelete(plugin)}
                        className="rounded px-3 py-1.5 text-xs font-medium text-forge-error hover:bg-forge-error/10 transition-colors"
                        title="Remove this plugin file and its blocks"
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                </li>
                );
              })}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}
