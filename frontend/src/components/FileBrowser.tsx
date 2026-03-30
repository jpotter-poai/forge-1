import { useCallback, useEffect, useRef, useState } from "react";
import { browseFiles, type BrowseResponse } from "@/api/client";
import type { BrowseMode } from "@/types/pipeline";

interface FileBrowserProps {
  /** Current value of the filepath field */
  initialPath: string;
  /** Whether we're opening a file, saving a file, or selecting a directory */
  mode?: BrowseMode | null;
  /** Called when user confirms a selection */
  onSelect: (path: string) => void;
  /** Called when user dismisses the dialog */
  onClose: () => void;
}

export function FileBrowser({
  initialPath,
  mode = "open_file",
  onSelect,
  onClose,
}: FileBrowserProps) {
  const [data, setData] = useState<BrowseResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<string | null>(null);
  const [filenameDraft, setFilenameDraft] = useState("");
  const didInitSaveDraft = useRef(false);

  const isDirectoryMode = mode === "directory";
  const isSaveMode = mode === "save_file";

  const loadDir = useCallback(async (path?: string) => {
    setLoading(true);
    setError(null);
    setSelectedEntry(null);
    try {
      const result = await browseFiles(path);
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to browse");
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    void loadDir(initialPath || undefined);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  useEffect(() => {
    if (!isSaveMode || !data) {
      return;
    }
    if (didInitSaveDraft.current) {
      return;
    }
    didInitSaveDraft.current = true;

    const normalizedInitial = initialPath.trim().replace(/[\\/]+$/, "");
    const normalizedCurrent = data.current.trim().replace(/[\\/]+$/, "");
    if (!normalizedInitial || normalizedInitial === normalizedCurrent) {
      setFilenameDraft("");
      return;
    }

    const inferredName = normalizedInitial.split(/[\\/]/).pop() ?? "";
    setFilenameDraft(inferredName);
  }, [data, initialPath, isSaveMode]);

  const handleEntryClick = (entry: { name: string; path: string; is_dir: boolean }) => {
    if (entry.is_dir) {
      void loadDir(entry.path);
    } else if (isSaveMode) {
      setSelectedEntry(entry.path);
      setFilenameDraft(entry.name);
    } else {
      setSelectedEntry(entry.path);
    }
  };

  const handleEntryDblClick = (entry: { name: string; path: string; is_dir: boolean }) => {
    if (entry.is_dir) {
      void loadDir(entry.path);
      return;
    }
    if (isSaveMode) {
      setSelectedEntry(entry.path);
      setFilenameDraft(entry.name);
    } else {
      onSelect(entry.path);
    }
  };

  const buildOutputPath = (directory: string, filename: string) => {
    const trimmed = filename.trim();
    if (!trimmed) {
      return "";
    }
    if (/^(?:[a-zA-Z]:[\\/]|\\\\|\/)/.test(trimmed)) {
      return trimmed;
    }
    const leaf = trimmed.replace(/^[/\\]+/, "");
    if (/[\\/]$/.test(directory)) {
      return `${directory}${leaf}`;
    }
    const separator = directory.includes("\\") ? "\\" : "/";
    return `${directory}${separator}${leaf}`;
  };

  const handleConfirm = () => {
    if (isDirectoryMode && data) {
      onSelect(data.current);
    } else if (isSaveMode && data) {
      const outputPath = buildOutputPath(data.current, filenameDraft);
      if (outputPath) {
        onSelect(outputPath);
      }
    } else if (selectedEntry) {
      onSelect(selectedEntry);
    }
  };

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={
        isDirectoryMode
          ? "Select Folder"
          : isSaveMode
            ? "Choose Output File"
            : "Select File"
      }
      className="fixed inset-0 z-50 flex items-start justify-center pt-16 bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="w-full max-w-lg bg-forge-surface border border-forge-border rounded-lg shadow-2xl overflow-hidden animate-fade-in-scale flex flex-col max-h-[70vh]">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-forge-border">
          <h2 className="text-forge-text font-semibold text-sm">
            {isDirectoryMode
              ? "Select Folder"
              : isSaveMode
                ? "Choose Output File"
                : "Select File"}
          </h2>
          <button
            onClick={onClose}
            className="text-forge-muted hover:text-forge-text text-sm px-1"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Current path breadcrumb */}
        {data && (
          <div className="px-4 py-2 border-b border-forge-border flex items-center gap-1.5 min-h-[36px]">
            {data.parent && (
              <button
                onClick={() => void loadDir(data.parent!)}
                className="text-forge-muted hover:text-forge-text text-xs px-1.5 py-0.5 rounded hover:bg-forge-border/40 transition-colors flex-shrink-0"
                title="Go up"
              >
                ↑ ..
              </button>
            )}
            <span className="text-forge-muted text-[11px] font-mono truncate" title={data.current}>
              {data.current}
            </span>
          </div>
        )}

        {/* File list */}
        <div className="flex-1 overflow-y-auto min-h-0">
          {loading && (
            <p className="text-forge-muted text-xs px-4 py-6 text-center animate-pulse">
              Loading…
            </p>
          )}
          {error && (
            <p className="text-forge-error text-xs px-4 py-4 text-center">{error}</p>
          )}
          {data && !loading && data.entries.length === 0 && (
            <p className="text-forge-muted text-xs px-4 py-6 text-center">
              Empty directory
            </p>
          )}
          {data && !loading && (
            <div className="py-1">
              {data.entries.map((entry) => (
                <button
                  key={entry.path}
                  onClick={() => handleEntryClick(entry)}
                  onDoubleClick={() => handleEntryDblClick(entry)}
                  className={`
                    w-full flex items-center gap-2 px-4 py-1.5 text-xs text-left
                    transition-colors duration-100
                    ${
                      selectedEntry === entry.path
                        ? "bg-forge-accent/15 text-forge-text"
                        : "text-forge-text hover:bg-forge-border/30"
                    }
                  `}
                >
                  <span className="flex-shrink-0 w-4 text-center text-forge-muted" aria-hidden="true">
                    {entry.is_dir ? "📁" : "📄"}
                  </span>
                  <span className="truncate">{entry.name}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between gap-2 px-4 py-3 border-t border-forge-border">
          <div className="min-w-0 flex-1">
            {isSaveMode ? (
              <div className="space-y-1">
                <label className="block text-[10px] text-forge-muted">
                  File name
                </label>
                <input
                  type="text"
                  value={filenameDraft}
                  onChange={(e) => setFilenameDraft(e.target.value)}
                  autoCapitalize="off"
                  autoCorrect="off"
                  spellCheck={false}
                  placeholder="result.csv"
                  className="w-full bg-forge-bg border border-forge-border rounded px-2 py-1.5 text-forge-text text-xs focus:outline-none focus:border-forge-accent transition-colors"
                />
              </div>
            ) : (
              <span className="text-[10px] text-forge-muted truncate block">
                {isDirectoryMode
                  ? "Open folders above, then select the current folder"
                  : selectedEntry
                    ? selectedEntry.split(/[\\/]/).pop()
                    : "Click a file to select"}
              </span>
            )}
          </div>
          <div className="flex gap-2 flex-shrink-0">
            <button
              onClick={onClose}
              className="btn-ghost text-xs"
            >
              Cancel
            </button>
            <button
              onClick={handleConfirm}
              disabled={
                isDirectoryMode
                  ? !data
                  : isSaveMode
                    ? !filenameDraft.trim()
                    : !selectedEntry
              }
              className={`
                px-3 py-1.5 rounded text-xs font-medium transition-colors
                ${
                  (isDirectoryMode && data) || (isSaveMode && filenameDraft.trim()) || selectedEntry
                    ? "bg-forge-accent hover:bg-forge-accent-hover text-white cursor-pointer"
                    : "bg-forge-border text-forge-muted cursor-not-allowed"
                }
              `}
            >
              {isSaveMode ? "Use Path" : "Select"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
