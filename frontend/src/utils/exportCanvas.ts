import { toPng } from "html-to-image";
import type { ReactFlowInstance } from "@xyflow/react";
import { jsPDF } from "jspdf";

const PADDING = 80;
// Background colour must be a literal hex/rgb — not a CSS variable — because
// we pass it directly to html-to-image outside of any stylesheet context.
const BG_COLOR = "#0f1117";
const WATERMARK_OPACITY = 0.15;

// Capture the full pipeline as a data URL.
//
// Strategy: target .react-flow__viewport directly (the element that holds all
// nodes AND edges) and pass the desired output dimensions + a custom transform
// via html-to-image's `style` option.  This matches the pattern shown in the
// official XY Flow docs and avoids any live-DOM mutations — no wrapper resizing,
// no setViewport calls, and no SVG-dimension workarounds needed (html-to-image
// creates the canvas at the explicit `width × height` we specify, so
// percentage-based child SVGs resolve correctly against that size).
async function captureFullPipeline(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  instance: ReactFlowInstance<any, any>,
  wrapperEl: HTMLElement,
): Promise<string> {
  const nodes = instance.getNodes();
  if (nodes.length === 0) throw new Error("No nodes to export.");

  // Compute bounding box of all nodes in flow-space
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  for (const node of nodes) {
    const x = node.position.x;
    const y = node.position.y;
    const w = (node.measured?.width ?? node.width ?? 180) as number;
    const h = (node.measured?.height ?? node.height ?? 80) as number;
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + w);
    maxY = Math.max(maxY, y + h);
  }

  const contentW = Math.ceil(maxX - minX + PADDING * 2);
  const contentH = Math.ceil(maxY - minY + PADDING * 2);

  // Translate so the top-left node lands at (PADDING, PADDING)
  const tx = -minX + PADDING;
  const ty = -minY + PADDING;

  const viewportEl = wrapperEl.querySelector<HTMLElement>(".react-flow__viewport");
  if (!viewportEl) throw new Error("Cannot find .react-flow__viewport");

  // ── Pre-resolve CSS custom-property colors onto SVG elements ────────────────
  // XY Flow v12 styles edges with:
  //   stroke: var(--xy-edge-stroke, var(--xy-edge-stroke-default))
  // The variable is defined on .react-flow in an external stylesheet.
  // html-to-image serialises into a foreignObject where that stylesheet is
  // absent, so the var() expression is unresolvable and stroke becomes
  // transparent.  Fix: read the computed (resolved) value in the live DOM and
  // stamp it as a literal SVG *attribute* on each path.  Attributes survive
  // cloneNode() and are not touched by html-to-image's CSS-copy logic.

  const SVG_SELECTORS = [
    ".react-flow__edge-path",
    ".react-flow__connection-path",
    ".react-flow__arrowhead polyline",
  ].join(", ");

  interface SvgAttrStash {
    el: SVGElement;
    stroke: string | null;
    fill: string | null;
    strokeWidth: string | null;
  }
  const svgStash: SvgAttrStash[] = [];

  viewportEl.querySelectorAll<SVGElement>(SVG_SELECTORS).forEach((el) => {
    const cs = window.getComputedStyle(el);
    svgStash.push({
      el,
      stroke: el.getAttribute("stroke"),
      fill: el.getAttribute("fill"),
      strokeWidth: el.getAttribute("stroke-width"),
    });
    // getComputedStyle resolves the CSS var chain to a concrete value
    if (cs.stroke && cs.stroke !== "none") el.setAttribute("stroke", cs.stroke);
    if (cs.fill)                           el.setAttribute("fill", cs.fill);
    if (cs.strokeWidth)                    el.setAttribute("stroke-width", cs.strokeWidth);
  });

  // toPng's `style` option overrides the cloned element's style before
  // serialisation — it does NOT mutate the live DOM.
  let dataUrl: string;
  try {
    dataUrl = await toPng(viewportEl, {
      backgroundColor: BG_COLOR,
      width: contentW,
      height: contentH,
      style: {
        width: `${contentW}px`,
        height: `${contentH}px`,
        transform: `translate(${tx}px, ${ty}px)`,
        transformOrigin: "0 0",
      },
      pixelRatio: 2,
    });
  } finally {
    // Restore original SVG attributes
    svgStash.forEach(({ el, stroke, fill, strokeWidth }) => {
      stroke      !== null ? el.setAttribute("stroke",       stroke)      : el.removeAttribute("stroke");
      fill        !== null ? el.setAttribute("fill",         fill)        : el.removeAttribute("fill");
      strokeWidth !== null ? el.setAttribute("stroke-width", strokeWidth) : el.removeAttribute("stroke-width");
    });
  }

  return dataUrl;
}

// Draw a subtle "Forge" watermark onto the captured image
function addWatermark(dataUrl: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement("canvas");
      canvas.width = img.width;
      canvas.height = img.height;
      const ctx = canvas.getContext("2d");
      if (!ctx) { resolve(dataUrl); return; }

      ctx.drawImage(img, 0, 0);

      const fontSize = Math.max(20, Math.round(img.height * 0.18));
      ctx.save();
      ctx.globalAlpha = WATERMARK_OPACITY;
      ctx.fillStyle = "#ffffff";
      ctx.font = `bold ${fontSize}px system-ui, -apple-system, sans-serif`;
      ctx.textAlign = "right";
      ctx.textBaseline = "top";
      ctx.fillText("Forge", img.width - 24, 24);
      ctx.restore();

      resolve(canvas.toDataURL("image/png"));
    };
    img.onerror = () => reject(new Error("Failed to load captured image for watermarking"));
    img.src = dataUrl;
  });
}

function triggerDownload(dataUrl: string, filename: string) {
  const a = document.createElement("a");
  a.href = dataUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function safeName(name: string): string {
  return name.replace(/[^a-z0-9_\-]/gi, "_").replace(/_+/g, "_").slice(0, 80);
}

// ── Public API ────────────────────────────────────────────────────────────────

export async function exportPipelinePng(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  instance: ReactFlowInstance<any, any>,
  wrapperEl: HTMLElement,
  pipelineName: string,
): Promise<void> {
  const raw = await captureFullPipeline(instance, wrapperEl);
  const watermarked = await addWatermark(raw);
  triggerDownload(watermarked, `${safeName(pipelineName)}_pipeline.png`);
}

export async function exportPipelinePdf(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  instance: ReactFlowInstance<any, any>,
  wrapperEl: HTMLElement,
  pipelineName: string,
): Promise<void> {
  const raw = await captureFullPipeline(instance, wrapperEl);
  const watermarked = await addWatermark(raw);

  // Load onto a temporary image to get natural dimensions
  const dims = await new Promise<{ w: number; h: number }>((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve({ w: img.naturalWidth, h: img.naturalHeight });
    img.onerror = reject;
    img.src = watermarked;
  });

  // A4 in points: 595.28 × 841.89 — choose landscape if wider than tall
  const landscape = dims.w >= dims.h;
  const pdf = new jsPDF({
    orientation: landscape ? "landscape" : "portrait",
    unit: "pt",
    format: "a4",
  });

  const pageW = pdf.internal.pageSize.getWidth();
  const pageH = pdf.internal.pageSize.getHeight();
  const margin = 20; // pt

  const availW = pageW - margin * 2;
  const availH = pageH - margin * 2;
  const scale = Math.min(availW / dims.w, availH / dims.h);
  const imgW = dims.w * scale;
  const imgH = dims.h * scale;
  const x = margin + (availW - imgW) / 2;
  const y = margin + (availH - imgH) / 2;

  pdf.addImage(watermarked, "PNG", x, y, imgW, imgH);
  pdf.save(`${safeName(pipelineName)}_pipeline.pdf`);
}
