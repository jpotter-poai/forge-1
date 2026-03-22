import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/globals.css";
import App from "./App";
import { ForgeShell } from "./ForgeShell";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ForgeShell>
      <App />
    </ForgeShell>
  </StrictMode>,
);
