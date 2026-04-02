import axios from "axios";
import type {
  BlockSpec,
  CancelExecutionResponse,
  CheckpointPreview,
  ExecuteResponse,
  Pipeline,
  PipelineEnvelope,
  PipelineSummary,
  StalenessResponse,
} from "@/types/pipeline";

/**
 * In Tauri mode, the backend runs on 127.0.0.1 and we need absolute URLs.
 * In browser dev mode, Vite proxies /api to the backend.
 *
 * Note: we lazily detect Tauri because __TAURI_INTERNALS__ may not be
 * available when this module is first evaluated.
 */
let _baseURL = "/api";
const TAURI_BACKEND_HOST = "127.0.0.1";

const http = axios.create({ baseURL: _baseURL });

export function setApiBaseUrl(port: number) {
  _baseURL = `http://${TAURI_BACKEND_HOST}:${port}/api`;
  http.defaults.baseURL = _baseURL;
}

/** Ensure we're pointing at the right backend. Call before first API use in Tauri. */
function ensureTauriBaseUrl() {
  if (
    _baseURL === "/api" &&
    typeof window !== "undefined" &&
    "__TAURI_INTERNALS__" in window
  ) {
    setApiBaseUrl(40964);
  }
}

// Intercept every request to ensure the base URL is correct
http.interceptors.request.use((config) => {
  ensureTauriBaseUrl();
  // Update baseURL in case it was just set
  config.baseURL = http.defaults.baseURL;
  return config;
});

function downloadBlob(blob: Blob, filename: string): void {
  const url = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
}

function filenameFromDisposition(
  header: string | undefined,
  fallback: string,
): string {
  if (!header) return fallback;
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    return decodeURIComponent(utf8Match[1]);
  }
  const quotedMatch = header.match(/filename="([^"]+)"/i);
  if (quotedMatch?.[1]) {
    return quotedMatch[1];
  }
  const plainMatch = header.match(/filename=([^;]+)/i);
  if (plainMatch?.[1]) {
    return plainMatch[1].trim();
  }
  return fallback;
}

// ── Blocks ────────────────────────────────────────────────────────────────────

export async function fetchBlocks(): Promise<BlockSpec[]> {
  const { data } = await http.get<BlockSpec[]>("/blocks");
  return data;
}

// ── Pipelines ─────────────────────────────────────────────────────────────────

export async function listPipelines(): Promise<PipelineSummary[]> {
  const { data } = await http.get<PipelineSummary[]>("/pipelines");
  return data;
}

export async function getPipeline(id: string): Promise<PipelineEnvelope> {
  const { data } = await http.get<PipelineEnvelope>(`/pipelines/${id}`);
  return data;
}

export async function createPipeline(
  pipeline: Pipeline,
  pipelineId?: string,
): Promise<PipelineEnvelope> {
  const params = pipelineId ? { pipeline_id: pipelineId } : {};
  const { data } = await http.post<PipelineEnvelope>("/pipelines", pipeline, {
    params,
  });
  return data;
}

export async function updatePipeline(
  id: string,
  pipeline: Pipeline,
): Promise<PipelineEnvelope> {
  const { data } = await http.put<PipelineEnvelope>(
    `/pipelines/${id}`,
    pipeline,
  );
  return data;
}

export async function prettifyPipeline(id: string): Promise<PipelineEnvelope> {
  const { data } = await http.post<PipelineEnvelope>(`/pipelines/${id}/prettify`);
  return data;
}

export async function deletePipeline(id: string): Promise<void> {
  await http.delete(`/pipelines/${id}`);
}

export async function getStaleness(id: string): Promise<StalenessResponse> {
  const { data } = await http.get<StalenessResponse>(
    `/pipelines/${id}/staleness`,
  );
  return data;
}

export async function downloadPipelineExport(
  id: string,
  format: "python" | "notebook",
): Promise<void> {
  const response = await http.get<Blob>(`/pipelines/${id}/export`, {
    params: { format },
    responseType: "blob",
  });
  const filename = filenameFromDisposition(
    response.headers["content-disposition"],
    `${id}_${format}.zip`,
  );
  downloadBlob(response.data, filename);
}

// ── Execution ─────────────────────────────────────────────────────────────────

export async function executePipeline(id: string): Promise<ExecuteResponse> {
  const { data } = await http.post<ExecuteResponse>(
    `/pipelines/${id}/execute`,
  );
  return data;
}

export async function cancelPipelineExecution(
  id: string,
): Promise<CancelExecutionResponse> {
  const { data } = await http.post<CancelExecutionResponse>(
    `/pipelines/${id}/cancel`,
  );
  return data;
}

// ── Checkpoints ───────────────────────────────────────────────────────────────

export async function getCheckpointPreview(
  checkpointId: string,
  limit = 50,
): Promise<CheckpointPreview> {
  const { data } = await http.get<CheckpointPreview>(
    `/checkpoints/${checkpointId}/preview`,
    { params: { limit } },
  );
  return data;
}

export interface CheckpointProvenance {
  images?: string[];
  [key: string]: unknown;
}

export async function getCheckpointProvenance(
  checkpointId: string,
): Promise<CheckpointProvenance> {
  const { data } = await http.get<CheckpointProvenance>(
    `/checkpoints/${checkpointId}/provenance`,
  );
  return data;
}

export function checkpointImageUrl(
  checkpointId: string,
  filename: string,
): string {
  ensureTauriBaseUrl();
  const encodedFilename = encodeURIComponent(filename);
  if (_baseURL.startsWith("http")) {
    return `${_baseURL}/checkpoints/${checkpointId}/images/${encodedFilename}`;
  }
  return `/api/checkpoints/${checkpointId}/images/${encodedFilename}`;
}

// ── File browser ─────────────────────────────────────────────────────────

export interface FileEntry {
  name: string;
  path: string;
  is_dir: boolean;
}

export interface BrowseResponse {
  current: string;
  parent: string | null;
  entries: FileEntry[];
}

export async function browseFiles(
  path?: string,
  showHidden = false,
): Promise<BrowseResponse> {
  const { data } = await http.get<BrowseResponse>("/files/browse", {
    params: { path: path || "", show_hidden: showHidden },
  });
  return data;
}

// ── Custom block plugins ──────────────────────────────────────────────────────

export interface CustomBlockEntry {
  filename: string;
  stem: string;
  path: string;
  requirements: string[];
  title: string;
  description: string;
  blocks: Array<{
    key: string;
    name: string;
    category: string;
    version: string;
  }>;
}

export interface PluginMetadata {
  title: string;
  description: string;
}

interface RawCustomBlockEntry {
  filename?: unknown;
  stem?: unknown;
  path?: unknown;
  requirements?: unknown;
  title?: unknown;
  description?: unknown;
  blocks?: unknown;
}

function humanizePluginTitle(stem: string): string {
  const parts = stem.split(/[\s_-]+/).filter(Boolean);
  if (parts.length === 0) return "Custom Plugin";
  return parts.map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(" ");
}

function defaultPluginDescription(filename: string): string {
  return `Custom block plugin installed from ${filename}.`;
}

function parseSimplePythonStringAssignment(source: string, key: string): string | null {
  const line = source
    .split(/\r?\n/)
    .find((entry) => new RegExp(`^\\s*${key}\\s*=`).test(entry));
  if (!line) {
    return null;
  }
  const match = line.match(/=\s*(["'])(.*)\1\s*$/);
  if (!match?.[2]) {
    return null;
  }

  return match[2]
    .replace(/\\\\/g, "\\")
    .replace(/\\n/g, "\n")
    .replace(/\\r/g, "\r")
    .replace(/\\t/g, "\t")
    .replace(/\\"/g, '"')
    .replace(/\\'/g, "'");
}

function parsePluginMetadataDictValue(
  source: string,
  field: "title" | "description",
): string | null {
  const metadataMatch = source.match(/PLUGIN_METADATA\s*=\s*\{([\s\S]*?)\}/m);
  if (!metadataMatch?.[1]) {
    return null;
  }
  const fieldPattern = new RegExp(
    `["']${field}["']\\s*:\\s*(["'])(.*?)\\1`,
    "m",
  );
  const match = metadataMatch[1].match(fieldPattern);
  return match?.[2]?.trim() || null;
}

export function extractPluginMetadataFromSource(
  source: string,
  filename: string,
): PluginMetadata {
  const stem = filename.replace(/\.py$/i, "");
  const title =
    parseSimplePythonStringAssignment(source, "PLUGIN_TITLE") ||
    parsePluginMetadataDictValue(source, "title") ||
    humanizePluginTitle(stem);
  const description =
    parseSimplePythonStringAssignment(source, "PLUGIN_DESCRIPTION") ||
    parsePluginMetadataDictValue(source, "description") ||
    defaultPluginDescription(filename);

  return {
    title,
    description,
  };
}

function templateHasPluginMetadata(source: string): boolean {
  return (
    /\bPLUGIN_TITLE\s*=/.test(source) ||
    /\bPLUGIN_DESCRIPTION\s*=/.test(source) ||
    /\bPLUGIN_METADATA\s*=/.test(source)
  );
}

function ensureTemplatePluginMetadata(source: string, blockName: string): string {
  if (templateHasPluginMetadata(source)) {
    return source;
  }

  const metadataSnippet =
    "\n# Optional plugin metadata shown in the Manage Plugins window.\n" +
    `PLUGIN_TITLE = ${JSON.stringify(blockName)}\n` +
    'PLUGIN_DESCRIPTION = "Describe what this plugin file provides."\n';

  const requirementsMatch = source.match(
    /(^REQUIREMENTS\s*(?::[^=\n]+)?=\s*\[[\s\S]*?\]\s*$)/m,
  );
  if (requirementsMatch?.index !== undefined) {
    const insertAt = requirementsMatch.index + requirementsMatch[0].length;
    return `${source.slice(0, insertAt)}${metadataSnippet}${source.slice(insertAt)}`;
  }

  const importMatch = source.match(/^(from\s+\S+\s+import\s+.+|import\s+.+)$/m);
  if (importMatch?.index !== undefined) {
    return `${source.slice(0, importMatch.index)}${metadataSnippet}\n${source.slice(importMatch.index)}`;
  }

  return `${source}${metadataSnippet}`;
}

function normalizeCustomBlockEntry(raw: RawCustomBlockEntry): CustomBlockEntry {
  const filename = typeof raw.filename === "string" && raw.filename.trim()
    ? raw.filename
    : "custom_plugin.py";
  const stem = typeof raw.stem === "string" && raw.stem.trim()
    ? raw.stem
    : filename.replace(/\.py$/i, "");
  const requirements = Array.isArray(raw.requirements)
    ? raw.requirements.filter((item): item is string => typeof item === "string")
    : [];
  const blocks = Array.isArray(raw.blocks)
    ? raw.blocks
        .filter(
          (item): item is { key?: unknown; name?: unknown; category?: unknown; version?: unknown } =>
            typeof item === "object" && item !== null,
        )
        .map((block) => ({
          key: typeof block.key === "string" ? block.key : "",
          name: typeof block.name === "string" ? block.name : "",
          category: typeof block.category === "string" ? block.category : "Custom",
          version: typeof block.version === "string" ? block.version : "1.0.0",
        }))
        .filter((block) => block.key.length > 0 && block.name.length > 0)
    : [];

  return {
    filename,
    stem,
    path: typeof raw.path === "string" ? raw.path : filename,
    requirements,
    title:
      typeof raw.title === "string" && raw.title.trim()
        ? raw.title
        : humanizePluginTitle(stem),
    description:
      typeof raw.description === "string" && raw.description.trim()
        ? raw.description
        : defaultPluginDescription(filename),
    blocks,
  };
}

export interface InstallBlockResult {
  success: boolean;
  conflict: boolean;
  suggested_filename: string | null;
  block_name: string;
  filename: string;
  installed_packages: string[];
  skipped_packages: string[];
  errors: string[];
  message: string;
}

export async function listCustomBlocks(): Promise<CustomBlockEntry[]> {
  const { data } = await http.get<RawCustomBlockEntry[]>("/custom-blocks");
  if (!Array.isArray(data)) return [];
  return data.map(normalizeCustomBlockEntry);
}

export async function installCustomBlock(
  file: File,
  conflictResolution?: "overwrite" | "rename",
): Promise<InstallBlockResult> {
  const formData = new FormData();
  formData.append("file", file);
  // Do NOT set Content-Type manually — axios must set it automatically so it
  // includes the multipart boundary; without the boundary FastAPI returns 422.
  const { data } = await http.post<InstallBlockResult>("/custom-blocks/install", formData, {
    params: conflictResolution ? { conflict_resolution: conflictResolution } : undefined,
  });
  return data;
}

export async function deleteCustomBlock(filename: string): Promise<void> {
  await http.delete(`/custom-blocks/${encodeURIComponent(filename)}`);
}

export async function getCustomBlockSource(filename: string): Promise<string> {
  const response = await http.get<Blob>(
    `/custom-blocks/${encodeURIComponent(filename)}/export`,
    { responseType: "blob" },
  );
  return response.data.text();
}

export async function downloadBlockTemplate(name = "My Custom Block"): Promise<string> {
  const response = await http.get<Blob>("/custom-blocks/template", {
    params: { name },
    responseType: "blob",
  });
  const source = await response.data.text();
  const normalizedSource = ensureTemplatePluginMetadata(source, name);
  const filename = filenameFromDisposition(
    response.headers["content-disposition"],
    "custom_block_template.py",
  );
  downloadBlob(
    new Blob([normalizedSource], { type: "text/x-python;charset=utf-8" }),
    filename,
  );
  return filename;
}

export async function exportCustomBlock(filename: string): Promise<void> {
  const response = await http.get<Blob>(
    `/custom-blocks/${encodeURIComponent(filename)}/export`,
    { responseType: "blob" },
  );
  const dlFilename = filenameFromDisposition(
    response.headers["content-disposition"],
    filename,
  );
  downloadBlob(response.data, dlFilename);
}

// ── MCP Config ───────────────────────────────────────────────────────────────

export interface McpConfigResponse {
  python_executable: string;
  pythonpath: string;
  blocks_dir: string;
  pipeline_dir: string;
  checkpoint_dir: string;
  log_level: string;
  config_json: Record<string, unknown>;
  setup_prompt: string;
  os_name: string;
}

export async function getMcpConfig(): Promise<McpConfigResponse> {
  const { data } = await http.get<McpConfigResponse>("/mcp-config");
  return data;
}

// ── WebSocket ─────────────────────────────────────────────────────────────────

export function openExecutionSocket(pipelineId: string): WebSocket {
  if (_baseURL.startsWith("http")) {
    const port = _baseURL.match(/:(\d+)/)?.[1] ?? "40964";
    return new WebSocket(
      `ws://${TAURI_BACKEND_HOST}:${port}/api/ws/execute/${pipelineId}`,
    );
  }
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  return new WebSocket(`${protocol}://${host}/api/ws/execute/${pipelineId}`);
}
