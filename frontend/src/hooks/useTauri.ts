/**
 * Hook for Tauri desktop integration.
 * When running in Tauri, manages backend lifecycle (Python detection, venv setup, server start).
 * When running in browser (dev mode), this is a no-op.
 */

import { useState, useEffect, useCallback, useRef } from "react";

export type SetupStage =
  | "checking" // Detecting Python
  | "creating-venv" // Creating virtual environment
  | "installing" // Installing dependencies
  | "starting" // Starting backend server
  | "ready" // Backend is up and running
  | "error" // Something went wrong
  | "no-python" // Python not found
  | "file-locked"; // Another process holds venv files

interface TauriState {
  /** Whether we're running inside Tauri (desktop app) vs browser */
  isTauri: boolean;
  /** Current setup stage */
  stage: SetupStage;
  /** Error message if stage is "error" */
  error: string | null;
  /** Backend port once ready */
  port: number | null;
  /** Trigger setup manually (e.g. after user installs Python) */
  retry: () => void;
}

/** Check if we're running inside a Tauri webview */
function detectTauri(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

export function useTauri(): TauriState {
  const isTauri = detectTauri();
  const [stage, setStage] = useState<SetupStage>(
    isTauri ? "checking" : "ready",
  );
  const [error, setError] = useState<string | null>(null);
  const [port, setPort] = useState<number | null>(isTauri ? null : 40964);

  // Guard against double-invocation (React StrictMode runs effects twice)
  const setupRunning = useRef(false);

  const runSetup = useCallback(async () => {
    if (!isTauri) return;

    // Prevent concurrent runs
    if (setupRunning.current) {
      console.log("[useTauri] Setup already running, skipping duplicate call");
      return;
    }
    setupRunning.current = true;

    try {
      setStage("checking");
      setError(null);

      const { invoke } = await import("@tauri-apps/api/core");

      // Reset server-side status (needed for retries)
      await invoke("reset_setup");
      const { listen } = await import("@tauri-apps/api/event");

      // Listen for status updates from the Rust backend.
      // The payload is a Serde-serialized Rust enum, which comes through as
      // either a string ("CreatingVenv") or an object ({"Ready": {"port": 40964}}).
      const unlisten = await listen<string | Record<string, unknown>>(
        "backend-status",
        (event) => {
          const p = event.payload;
          console.log("[useTauri] backend-status event:", p);

          if (p === "CreatingVenv") setStage("creating-venv");
          else if (p === "InstallingDeps") setStage("installing");
          else if (p === "StartingServer") setStage("starting");
          else if (typeof p === "object" && p !== null && "Ready" in p) {
            setStage("ready");
          }
        },
      );

      // Check for Python first
      try {
        await invoke<string>("check_python");
      } catch {
        setStage("no-python");
        setupRunning.current = false;
        unlisten();
        return;
      }

      // Run setup and start
      const backendPort = await invoke<number>("setup_and_start");
      setPort(backendPort);
      setStage("ready");
      // Don't unlisten — we may still want status events
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
      setStage("error");
    } finally {
      setupRunning.current = false;
    }
  }, [isTauri]);

  useEffect(() => {
    runSetup();
  }, [runSetup]);

  return { isTauri, stage, error, port, retry: runSetup };
}
