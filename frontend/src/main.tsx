import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/globals.css";
import App from "./App";
import { ForgeShell } from "./ForgeShell";

window.addEventListener("error", (event) => {
  console.error("[frontend][window.error]", {
    message: event.message,
    filename: event.filename,
    lineno: event.lineno,
    colno: event.colno,
    error: event.error,
  });
});

window.addEventListener("unhandledrejection", (event) => {
  console.error("[frontend][unhandledrejection]", event.reason);
});

window.addEventListener("keydown", async (event) => {
  const wantsDevtools =
    event.key === "F12" ||
    ((event.ctrlKey || event.metaKey) &&
      event.altKey &&
      event.key.toLowerCase() === "i");
  if (!wantsDevtools) return;
  if (!("__TAURI_INTERNALS__" in window)) return;

  event.preventDefault();
  try {
    const { invoke } = await import("@tauri-apps/api/core");
    await invoke("open_devtools");
    console.info("[frontend] Requested Tauri devtools");
  } catch (error) {
    console.error("[frontend] Failed to open Tauri devtools", error);
  }
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ForgeShell>
      <App />
    </ForgeShell>
  </StrictMode>,
);
