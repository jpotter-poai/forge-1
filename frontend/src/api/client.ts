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

/** Detect if running inside Tauri desktop shell */
const IS_TAURI =
  typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

/**
 * In Tauri mode, the backend runs on 127.0.0.1:40964 and we need absolute URLs.
 * In browser dev mode, Vite proxies /api to the backend.
 */
let _baseURL = "/api";

export function setApiBaseUrl(port: number) {
  _baseURL = `http://127.0.0.1:${port}/api`;
  http.defaults.baseURL = _baseURL;
}

const http = axios.create({
  baseURL: IS_TAURI ? "http://127.0.0.1:40964/api" : "/api",
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

export function checkpointImageUrl(
  checkpointId: string,
  filename: string,
): string {
  if (IS_TAURI) {
    return `http://127.0.0.1:${_baseURL.match(/:(\d+)/)?.[1] ?? "40964"}/api/checkpoints/${checkpointId}/images/${filename}`;
  }
  return `/api/checkpoints/${checkpointId}/images/${filename}`;
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

// ── WebSocket ─────────────────────────────────────────────────────────────────

export function openExecutionSocket(pipelineId: string): WebSocket {
  if (IS_TAURI) {
    const port = _baseURL.match(/:(\d+)/)?.[1] ?? "40964";
    return new WebSocket(`ws://127.0.0.1:${port}/api/ws/execute/${pipelineId}`);
  }
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  return new WebSocket(`${protocol}://${host}/api/ws/execute/${pipelineId}`);
}
