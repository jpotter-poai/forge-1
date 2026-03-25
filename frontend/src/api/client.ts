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

const http = axios.create({ baseURL: _baseURL });

export function setApiBaseUrl(port: number) {
  _baseURL = `http://localhost:${port}/api`;
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
}

export interface InstallBlockResult {
  success: boolean;
  block_name: string;
  filename: string;
  installed_packages: string[];
  skipped_packages: string[];
  errors: string[];
  message: string;
}

export async function listCustomBlocks(): Promise<CustomBlockEntry[]> {
  const { data } = await http.get<CustomBlockEntry[]>("/custom-blocks");
  return data;
}

export async function installCustomBlock(file: File): Promise<InstallBlockResult> {
  const formData = new FormData();
  formData.append("file", file);
  const { data } = await http.post<InstallBlockResult>("/custom-blocks/install", formData, {
    headers: { "Content-Type": "multipart/form-data" },
  });
  return data;
}

export async function deleteCustomBlock(filename: string): Promise<void> {
  await http.delete(`/custom-blocks/${encodeURIComponent(filename)}`);
}

export async function downloadBlockTemplate(name = "My Custom Block"): Promise<void> {
  const response = await http.get<Blob>("/custom-blocks/template", {
    params: { name },
    responseType: "blob",
  });
  const filename = filenameFromDisposition(
    response.headers["content-disposition"],
    "custom_block_template.py",
  );
  downloadBlob(response.data, filename);
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
    return new WebSocket(`ws://localhost:${port}/api/ws/execute/${pipelineId}`);
  }
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  return new WebSocket(`${protocol}://${host}/api/ws/execute/${pipelineId}`);
}
