import { useCallback, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import type { CSSProperties, ReactNode } from "react";
import type { Node, Edge } from "@xyflow/react";
import type { ForgeNodeData } from "@/hooks/usePipeline";

// ── Constants ─────────────────────────────────────────────────────────────────

const SPOT_PAD = 10;
const SCREEN_PAD = 16;
const TOOLTIP_W = 300;
const TOOLTIP_H = 280;
const FINALE_W = 420;

// Characters defined via fromCharCode so the Write tool doesn't escape them
const CH_LEFT = String.fromCharCode(0x2190); // left arrow
const CH_RIGHT = String.fromCharCode(0x2192); // right arrow
const CH_CHECK = String.fromCharCode(0x2713); // check mark
const CH_HAMMER = String.fromCharCode(0x2692); // hammer

// ── Types ─────────────────────────────────────────────────────────────────────

export interface TourState {
  nodes: Node<ForgeNodeData>[];
  edges: Edge[];
  runCount: number;
  completedRunCount: number;
  paramsChangeCount: number;
}

type TourDescription = ReactNode | ((s: TourState) => ReactNode);

interface TourStep {
  /** CSS selector to spotlight, or null for no spotlight. */
  target: string | null;
  title: string;
  description: TourDescription;
  /** Shown beneath description when action isn't complete yet. */
  actionHint: string | null;
  /** Returns true when the user has completed this step's required action. */
  isComplete: (s: TourState) => boolean;
  /** Renders the finale card instead of the standard tooltip. */
  isFinale?: boolean;
  /** Remove the dimming overlay once the step action is complete. */
  clearOverlayOnComplete?: boolean;
  /** Remove the dimming overlay when a custom condition is met. */
  clearOverlayWhen?: (s: TourState) => boolean;
  /**
   * When true, the overlay is completely removed and the tooltip parks in the
   * bottom-right corner so the user can work freely on the canvas + palette.
   * Use for steps where the user needs to drag, connect, or click on nodes.
   */
  noOverlay?: boolean;
  /** Preferred side for free-roam tooltip docking. */
  dockSide?: "left" | "right";
  /** When set, glow a specific palette block during this step. */
  highlightPaletteBlockKey?: string;
  /** When true, inject a pulsing animation on React Flow handles. */
  pulseHandles?: boolean;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function byBlockKey(nodes: Node<ForgeNodeData>[], key: string) {
  return nodes.filter(
    (n) => n.type === "forgeBlock" && n.data?.blockKey === key,
  );
}

function normalizeTourPath(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.trim().replace(/\\/g, "/").toLowerCase();
}

function pathMatchesBasename(value: unknown, basename: string): boolean {
  const normalized = normalizeTourPath(value);
  if (!normalized) return false;
  return normalized === basename || normalized.endsWith(`/${basename}`);
}

function findConnectedTargetNode(
  state: TourState,
  sourceKey: string,
  targetKey: string,
): Node<ForgeNodeData> | null {
  const sourceIds = new Set(byBlockKey(state.nodes, sourceKey).map((node) => node.id));
  const targets = byBlockKey(state.nodes, targetKey);
  if (targets.length === 0) return null;
  const targetIds = new Set(targets.map((node) => node.id));
  const connectedTargetId = state.edges.find(
    (edge) => sourceIds.has(edge.source) && targetIds.has(edge.target),
  )?.target;
  return (
    targets.find((node) => node.id === connectedTargetId) ??
    targets[0] ??
    null
  );
}

function hasConnection(
  state: TourState,
  sourceKey: string,
  targetKey: string,
): boolean {
  const sourceIds = new Set(byBlockKey(state.nodes, sourceKey).map((node) => node.id));
  const targetIds = new Set(byBlockKey(state.nodes, targetKey).map((node) => node.id));
  if (sourceIds.size === 0 || targetIds.size === 0) return false;
  return state.edges.some(
    (edge) => sourceIds.has(edge.source) && targetIds.has(edge.target),
  );
}

function readStringParam(
  node: Node<ForgeNodeData> | null,
  key: string,
): string {
  if (!node) return "";
  const value = node.data?.params?.[key];
  return typeof value === "string" ? value.trim() : "";
}

interface CountProgress {
  firstDone: boolean;
  secondDone: boolean;
  complete: boolean;
}

function getLoadCsvProgress(state: TourState): CountProgress {
  const count = byBlockKey(state.nodes, "LoadCSV").length;
  return {
    firstDone: count >= 1,
    secondDone: count >= 2,
    complete: count >= 2,
  };
}

function getAppendConnectionProgress(state: TourState): CountProgress {
  const loaderIds = new Set(byBlockKey(state.nodes, "LoadCSV").map((node) => node.id));
  const appendIds = new Set(
    byBlockKey(state.nodes, "AppendDatasets").map((node) => node.id),
  );
  const connectedLoaderIds = new Set(
    state.edges
      .filter((edge) => loaderIds.has(edge.source) && appendIds.has(edge.target))
      .map((edge) => edge.source),
  );
  const count = connectedLoaderIds.size;
  return {
    firstDone: count >= 1,
    secondDone: count >= 2,
    complete: count >= 2,
  };
}

interface AddAndConnectProgress {
  added: boolean;
  connected: boolean;
  complete: boolean;
}

function getAddAndConnectProgress(
  state: TourState,
  sourceKey: string,
  targetKey: string,
): AddAndConnectProgress {
  const added = byBlockKey(state.nodes, targetKey).length >= 1;
  const connected = hasConnection(state, sourceKey, targetKey);
  return {
    added,
    connected,
    complete: added && connected,
  };
}

interface ConfigureProgress {
  loader1Ready: boolean;
  loader2Ready: boolean;
  kmeansReady: boolean;
  xColumnReady: boolean;
  yColumnReady: boolean;
  colorColumnReady: boolean;
  colorModeReady: boolean;
  complete: boolean;
}

function getConfigureProgress(state: TourState): ConfigureProgress {
  const csv = byBlockKey(state.nodes, "LoadCSV");
  const loader1Ready = csv.some((node) =>
    pathMatchesBasename(node.data?.params?.filepath, "toy_1.csv"),
  );
  const loader2Ready = csv.some((node) =>
    pathMatchesBasename(node.data?.params?.filepath, "toy_2.csv"),
  );

  const kmeansNode = findConnectedTargetNode(
    state,
    "AppendDatasets",
    "KMeansClustering",
  );
  const kmeansReady =
    kmeansNode != null && Number(kmeansNode.data?.params?.n_clusters) === 4;

  const scatterNode = findConnectedTargetNode(
    state,
    "KMeansClustering",
    "MatrixScatterPlot",
  );
  const xColumnReady = readStringParam(scatterNode, "x_column") === "x";
  const yColumnReady = readStringParam(scatterNode, "y_column") === "y";
  const colorColumnReady =
    readStringParam(scatterNode, "color_column") === "cluster_id";
  const colorModeReady =
    readStringParam(scatterNode, "color_mode") === "categorical";

  return {
    loader1Ready,
    loader2Ready,
    kmeansReady,
    xColumnReady,
    yColumnReady,
    colorColumnReady,
    colorModeReady,
    complete:
      loader1Ready &&
      loader2Ready &&
      kmeansReady &&
      xColumnReady &&
      yColumnReady &&
      colorColumnReady &&
      colorModeReady,
  };
}

interface RunProgress {
  started: boolean;
  finished: boolean;
  complete: boolean;
}

function getRunProgress(state: TourState, threshold: number): RunProgress {
  const started = state.runCount >= threshold;
  const finished = state.completedRunCount >= threshold;
  return {
    started,
    finished,
    complete: finished,
  };
}

function TourChecklistItem({
  done,
  label,
  value,
}: {
  done: boolean;
  label: string;
  value?: string;
}) {
  return (
    <span className="block ml-1 mt-1 text-xs">
      <span
        className={`inline-flex w-4 items-center justify-center ${
          done ? "text-emerald-400" : "text-forge-muted"
        }`}
      >
        {done ? CH_CHECK : "○"}
      </span>
      <span className="text-forge-text">{label}</span>
      {value ? (
        <>
          :{" "}
          <code className="text-[#818cf8] bg-forge-bg px-1 py-0.5 rounded">
            {value}
          </code>
        </>
      ) : null}
    </span>
  );
}

// ── Step definitions ──────────────────────────────────────────────────────────
//
// Overlay rules:
//
//   spotlight  = dim everything except one element (palette section, run button)
//               Good when the user only needs ONE region.
//               Overlay auto-clears during drag (isDragging prop).
//
//   noOverlay  = no dimming at all, tooltip in bottom-right corner
//               Good when the user needs palette + canvas simultaneously
//               (dragging, connecting, clicking nodes, editing params).
//
//   full dim   = dark overlay + centered card (only used for finale)
//

const TOUR_STEPS: TourStep[] = [
  // ── 0: Load two datasets ───────────────────────────────────────────────────
  {
    target: '[data-tour-block-key="LoadCSV"]',
    title: "Add Import Blocks",
    highlightPaletteBlockKey: "LoadCSV",
    description: (s) => {
      const progress = getLoadCsvProgress(s);
      return (
        <>
          <span className="block">
            Start by dragging two{" "}
            <strong className="text-forge-text">Load CSV</strong> blocks onto the
            canvas.
          </span>
          <TourChecklistItem
            done={progress.firstDone}
            label="Added first Load CSV Block"
          />
          <TourChecklistItem
            done={progress.secondDone}
            label="Added second Load CSV Block"
          />
        </>
      );
    },
    actionHint: "Drag both Load CSV blocks onto the canvas",
    isComplete: (s) => getLoadCsvProgress(s).complete,
  },

  // ── 1: Add append block ────────────────────────────────────────────────────
  {
    target: '[data-tour="block-palette"]',
    title: "Add an Append Datasets block",
    description:
      "Find Append Datasets in the Operator section and drag it onto the canvas. " +
      "This will stack the two CSVs into a single table.",
    actionHint: "Drag an Append Datasets block onto the canvas",
    isComplete: (s) => byBlockKey(s.nodes, "AppendDatasets").length >= 1,
  },

  // ── 2: Wire loaders to append ──────────────────────────────────────────────
  {
    target: null,
    noOverlay: true,
    pulseHandles: true,
    title: "Wire the loaders to append",
    description: (s) => {
      const progress = getAppendConnectionProgress(s);
      return (
        <>
          <span className="block">
            Drag from the output handle (right side) of each{" "}
            <strong className="text-forge-text">Load CSV</strong> block to the
            inputs on <strong className="text-forge-text">Append Datasets</strong>.
          </span>
          <TourChecklistItem
            done={progress.firstDone}
            label="Connected first loader to Append Datasets"
          />
          <TourChecklistItem
            done={progress.secondDone}
            label="Connected second loader to Append Datasets"
          />
        </>
      );
    },
    actionHint: "Connect both loader outputs into the append block's inputs",
    isComplete: (s) => getAppendConnectionProgress(s).complete,
  },

  // ── 3: Add K-Means clustering ──────────────────────────────────────────────
  {
    target: null,
    noOverlay: true,
    pulseHandles: true,
    title: "Add K-Means clustering",
    highlightPaletteBlockKey: "palette-search",
    description: (s) => {
      const progress = getAddAndConnectProgress(
        s,
        "AppendDatasets",
        "KMeansClustering",
      );
      return (
        <>
          <span className="block">
            Drag a <strong className="text-forge-text">K-Means Clustering</strong>{" "}
            block onto the canvas, then connect{" "}
            <strong className="text-forge-text">Append Datasets</strong> to it.
            Consider trying the search bar.
          </span>
          <TourChecklistItem
            done={progress.added}
            label="Added K-Means Clustering block"
          />
          <TourChecklistItem
            done={progress.connected}
            label="Connected Append Datasets to K-Means"
          />
        </>
      );
    },
    actionHint: "Add K-Means Clustering and connect append " + CH_RIGHT + " clustering",
    isComplete: (s) =>
      getAddAndConnectProgress(s, "AppendDatasets", "KMeansClustering").complete,
  },

  // ── 4: Add visualization ───────────────────────────────────────────────────
  {
    target: null,
    noOverlay: true,
    pulseHandles: true,
    title: "Visualize the clusters",
    description: (s) => {
      const progress = getAddAndConnectProgress(
        s,
        "KMeansClustering",
        "MatrixScatterPlot",
      );
      return (
        <>
          <span className="block">
            Drag a <strong className="text-forge-text">Matrix Scatter Plot</strong>{" "}
            block onto the canvas, then connect{" "}
            <strong className="text-forge-text">K-Means Clustering</strong> to it.
          </span>
          <TourChecklistItem
            done={progress.added}
            label="Added Matrix Scatter Plot block"
          />
          <TourChecklistItem
            done={progress.connected}
            label="Connected K-Means to Matrix Scatter Plot"
          />
        </>
      );
    },
    actionHint: "Add Matrix Scatter Plot and connect clustering " + CH_RIGHT + " viz",
    isComplete: (s) =>
      getAddAndConnectProgress(s, "KMeansClustering", "MatrixScatterPlot").complete,
  },

  // ── 5: Configure parameters ────────────────────────────────────────────────
  // User needs canvas (click nodes) + inspector (edit params). No overlay.
  {
    target: null,
    noOverlay: true,
    dockSide: "left",
    title: "Configure the blocks",
    description: (s) => {
      const progress = getConfigureProgress(s);
      return (
        <>
          <span className="block">
            Click each{" "}
            <strong className="text-forge-text">Load CSV</strong> block and set its{" "}
            <code className="text-[#818cf8] text-xs bg-forge-bg px-1 py-0.5 rounded">
              filepath
            </code>{" "}
            parameter:
          </span>
          <TourChecklistItem
            done={progress.loader1Ready}
            label="First loader"
            value="toy_datasets/toy_1.csv"
          />
          <TourChecklistItem
            done={progress.loader2Ready}
            label="Second loader"
            value="toy_datasets/toy_2.csv"
          />

          <span className="block mt-2.5">
            Then click{" "}
            <strong className="text-forge-text">K-Means Clustering</strong> and set:
          </span>
          <TourChecklistItem
            done={progress.kmeansReady}
            label="n_clusters"
            value="4"
          />

          <span className="block mt-2.5">
            Finally, click{" "}
            <strong className="text-forge-text">Matrix Scatter Plot</strong> and set:
          </span>
          <TourChecklistItem
            done={progress.xColumnReady}
            label="X Column"
            value="x"
          />
          <TourChecklistItem
            done={progress.yColumnReady}
            label="Y Column"
            value="y"
          />
          <TourChecklistItem
            done={progress.colorColumnReady}
            label="Color Column"
            value="cluster_id"
          />
          <TourChecklistItem
            done={progress.colorModeReady}
            label="Color Mode"
            value="categorical"
          />
        </>
      );
    },
    actionHint:
      "Set both CSV filepaths, n_clusters to 4, and configure the Matrix Scatter Plot fields",
    isComplete: (s) => getConfigureProgress(s).complete,
  },

  // ── 6: Run the pipeline ────────────────────────────────────────────────────
  {
    target: '[data-tour="run-button"]',
    title: "Run the pipeline",
    description: (s) => {
      const progress = getRunProgress(s, 1);
      return (
        <>
          <span className="block">
            Hit Run. Forge executes each block in order and caches the result at
            every step.
          </span>
          <TourChecklistItem
            done={progress.started}
            label="Started pipeline run"
          />
          <TourChecklistItem
            done={progress.finished}
            label="Pipeline finished running"
          />
        </>
      );
    },
    actionHint: "Click Run and wait for the pipeline to finish",
    clearOverlayWhen: (s) => getRunProgress(s, 1).started,
    isComplete: (s) => getRunProgress(s, 1).complete,
  },

  // ── 7: Tune a parameter ────────────────────────────────────────────────────
  // User needs canvas + inspector. No overlay.
  // Detect that n_clusters is no longer 4 (the value they just set).
  {
    target: null,
    noOverlay: true,
    title: "Tune a parameter",
    description:
      "Click the K-Means block again. Change n_clusters to a different value (try 2 or 6)" +
      " to see how the clustering changes. Notice how downstream blocks go stale.",
    actionHint: "Change n_clusters on the K-Means block",
    isComplete: (s) => {
      const km = byBlockKey(s.nodes, "KMeansClustering");
      return (
        km.length >= 1 &&
        km.some((n) => {
          const k = n.data?.params?.n_clusters;
          return k != null && Number.isFinite(Number(k)) && Number(k) !== 4;
        })
      );
    },
  },

  // ── 8: Run only what changed ───────────────────────────────────────────────
  {
    target: '[data-tour="run-button"]',
    title: "Run only what changed",
    description: (s) => {
      const progress = getRunProgress(s, 2);
      return (
        <>
          <span className="block">
            Hit Run again. Only K-Means and everything downstream should
            re-execute, while the loaders and append stay cached.
          </span>
          <TourChecklistItem
            done={progress.started}
            label="Started re-run"
          />
          <TourChecklistItem
            done={progress.finished}
            label="Pipeline finished running"
          />
        </>
      );
    },
    actionHint: "Click Run again and wait for it to finish",
    clearOverlayWhen: (s) => getRunProgress(s, 2).started,
    isComplete: (s) => getRunProgress(s, 2).complete,
  },

  // ── 9: Finale ──────────────────────────────────────────────────────────────
  {
    target: null,
    title: "This is the Forge advantage.",
    description: "",
    actionHint: null,
    isComplete: () => true,
    isFinale: true,
  },
];

// ── Handle pulse CSS ──────────────────────────────────────────────────────────

const HANDLE_PULSE_CSS = `
  .react-flow__handle {
    animation: forge-handle-pulse 1.6s ease-in-out infinite !important;
  }
  @keyframes forge-handle-pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(99,102,241,0.7); }
    50%      { box-shadow: 0 0 0 10px rgba(99,102,241,0); }
  }
`;

function getPaletteGlowCss(blockKey: string) {
  return `
    [data-tour-block-key="${blockKey}"] {
      border-color: rgba(129, 140, 248, 0.95) !important;
      box-shadow:
        0 0 0 1px rgba(216, 220, 253, 0.78),
        0 0 26px rgba(187, 188, 245, 0.81) !important;
      animation: forge-palette-glow 1.6s ease-in-out infinite !important;
    }
    @keyframes forge-palette-glow {
      0%, 100% {
        transform: translateZ(0);
        box-shadow:
          0 0 0 1px rgba(129, 140, 248, 0.45),
          0 0 18px rgba(99, 102, 241, 0.28);
      }
      50% {
        transform: translateZ(0) scale(1.05);
        box-shadow:
          0 0 0 1px rgba(129, 140, 248, 0.7),
          0 0 32px rgba(99, 102, 241, 0.55);
      }
    }
  `;
}

// ── Spotlight rect ─────────────────────────────────────────────────────────────

interface SpotRect {
  top: number;
  left: number;
  width: number;
  height: number;
}

// ── Component ─────────────────────────────────────────────────────────────────

export interface OnboardingTourProps {
  onDone: () => void;
  onSkip: () => void;
  nodes: Node<ForgeNodeData>[];
  edges: Edge[];
  runCount: number;
  completedRunCount: number;
  paramsChangeCount: number;
  /** True while the user is dragging a block from the palette. */
  isDragging: boolean;
}

export function OnboardingTour({
  onDone,
  onSkip,
  nodes,
  edges,
  runCount,
  completedRunCount,
  paramsChangeCount,
  isDragging,
}: OnboardingTourProps) {
  const [step, setStep] = useState(0);
  const [rect, setRect] = useState<SpotRect | null>(null);

  const currentStep = TOUR_STEPS[step];
  const isFirst = step === 0;
  const isLast = step === TOUR_STEPS.length - 1;

  const state: TourState = useMemo(
    () => ({ nodes, edges, runCount, completedRunCount, paramsChangeCount }),
    [nodes, edges, runCount, completedRunCount, paramsChangeCount],
  );

  const actionDone = useMemo(
    () => currentStep.isComplete(state),
    [currentStep, state],
  );
  const stepDescription = useMemo(
    () =>
      typeof currentStep.description === "function"
        ? currentStep.description(state)
        : currentStep.description,
    [currentStep, state],
  );

  // Recompute spotlight rect whenever step changes
  useEffect(() => {
    if (!currentStep.target) {
      setRect(null);
      return;
    }
    const el = document.querySelector(currentStep.target);
    if (!el) {
      setRect(null);
      return;
    }
    const r = el.getBoundingClientRect();
    setRect({ top: r.top, left: r.left, width: r.width, height: r.height });
  }, [step, currentStep.target]);

  const handleDone = useCallback(() => onDone(), [onDone]);
  const handleSkip = useCallback(() => onSkip(), [onSkip]);

  const handleNext = useCallback(() => {
    if (!actionDone) return;
    if (isLast) handleDone();
    else setStep((s) => s + 1);
  }, [actionDone, isLast, handleDone]);

  const handleBack = useCallback(() => {
    setStep((s) => Math.max(0, s - 1));
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") handleSkip();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handleSkip]);

  const shouldClearOverlay = useMemo(
    () =>
      currentStep.clearOverlayWhen?.(state) ??
      Boolean(currentStep.clearOverlayOnComplete && actionDone),
    [actionDone, currentStep, state],
  );

  const showOverlay =
    !currentStep.noOverlay &&
    !currentStep.isFinale &&
    !isDragging &&
    !shouldClearOverlay;

  return createPortal(
    <div
      className="fixed inset-0"
      style={{ zIndex: 9995, pointerEvents: "none" }}
      aria-label={`Tour step ${step + 1} of ${TOUR_STEPS.length}`}
    >
      {/* Handle pulse animation */}
      {currentStep.pulseHandles && <style>{HANDLE_PULSE_CSS}</style>}
      {currentStep.highlightPaletteBlockKey && (
        <style>{getPaletteGlowCss(currentStep.highlightPaletteBlockKey)}</style>
      )}

      {/* Spotlight / overlay */}
      {showOverlay &&
        (rect ? (
          <div
            aria-hidden="true"
            style={{
              position: "fixed",
              top: rect.top - SPOT_PAD,
              left: rect.left - SPOT_PAD,
              width: rect.width + SPOT_PAD * 2,
              height: rect.height + SPOT_PAD * 2,
              borderRadius: 8,
              boxShadow: "0 0 0 9999px rgba(0,0,0,0.72)",
              pointerEvents: "none",
            }}
          />
        ) : (
          <div
            aria-hidden="true"
            className="fixed inset-0 bg-black/55"
            style={{ pointerEvents: "none" }}
          />
        ))}

      {/* Finale overlay */}
      {currentStep.isFinale && (
        <div
          aria-hidden="true"
          className="fixed inset-0 bg-black/55"
          style={{ pointerEvents: "none" }}
        />
      )}

      {/* Tooltip / Finale card */}
      {currentStep.isFinale ? (
        <FinaleCard onDone={handleDone} />
      ) : (
        <TooltipCard
          step={currentStep}
          description={stepDescription}
          stepIndex={step}
          totalSteps={TOUR_STEPS.length - 1}
          rect={rect}
          actionDone={actionDone}
          isFirst={isFirst}
          isLast={isLast}
          onNext={handleNext}
          onBack={handleBack}
          onSkip={handleSkip}
        />
      )}
    </div>,
    document.body,
  );
}

// ── Tooltip card ──────────────────────────────────────────────────────────────

interface TooltipCardProps {
  step: TourStep;
  description: ReactNode;
  stepIndex: number;
  totalSteps: number;
  rect: SpotRect | null;
  actionDone: boolean;
  isFirst: boolean;
  isLast: boolean;
  onNext: () => void;
  onBack: () => void;
  onSkip: () => void;
}

const TOUR_CARD_STYLE: CSSProperties = {
  borderColor: "rgba(129, 140, 248, 0.75)",
  boxShadow:
    "0 0 0 1px rgba(129, 140, 248, 0.3), 0 0 28px rgba(99, 102, 241, 0.38), 0 24px 60px rgba(0, 0, 0, 0.5)",
};

function TooltipCard({
  step,
  description,
  stepIndex,
  totalSteps,
  rect,
  actionDone,
  isFirst,
  isLast,
  onNext,
  onBack,
  onSkip,
}: TooltipCardProps) {
  const style = getTooltipStyle(
    rect,
    TOOLTIP_W,
    TOOLTIP_H,
    step.noOverlay,
    step.dockSide,
  );

  return (
    <div
      className="fixed w-[300px] bg-forge-surface border rounded-xl animate-fade-in-scale"
      style={{
        ...style,
        ...TOUR_CARD_STYLE,
        zIndex: 9998,
        pointerEvents: "auto",
      }}
      role="dialog"
      aria-modal="false"
      aria-live="polite"
    >
      <div className="h-px bg-gradient-to-r from-transparent via-[#6366f1] to-transparent rounded-t-xl" />

      <div className="p-4">
        {/* Progress + skip */}
        <div className="flex items-center justify-between">
          <div
            className="flex items-center gap-1"
            aria-label={`Step ${stepIndex + 1} of ${totalSteps}`}
          >
            {Array.from({ length: totalSteps }).map((_, i) => (
              <div
                key={i}
                className="rounded-full transition-all duration-300"
                style={{
                  height: 3,
                  width: i === stepIndex ? 14 : 6,
                  background:
                    i === stepIndex
                      ? "#6366f1"
                      : i < stepIndex
                        ? "rgba(99,102,241,0.4)"
                        : "#2a2d3a",
                }}
              />
            ))}
          </div>
          <button
            onClick={onSkip}
            className="text-forge-muted hover:text-forge-text text-xs transition-colors duration-150 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-forge-border rounded"
          >
            Skip tour
          </button>
        </div>

        <h3 className="text-forge-text font-semibold text-sm mt-3 leading-snug">
          {step.title}
        </h3>
        <div className="text-forge-muted text-sm mt-1.5 leading-relaxed">
          {description}
        </div>

        {/* Action hint */}
        {step.actionHint && (
          <p
            className="text-[11px] mt-2.5 font-medium transition-colors duration-300"
            style={{ color: actionDone ? "#22c55e" : "#6366f1" }}
          >
            {actionDone ? `${CH_CHECK} Done!` : step.actionHint}
          </p>
        )}

        {/* Back / Next */}
        <div className="mt-4 flex gap-2">
          <button
            onClick={onBack}
            disabled={isFirst}
            className="px-3 py-1.5 rounded-lg border border-forge-border text-forge-muted hover:text-forge-text hover:border-forge-border-mid text-sm transition-colors duration-150 disabled:opacity-30 disabled:cursor-not-allowed focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-forge-border"
          >
            {CH_LEFT}
          </button>
          <button
            onClick={onNext}
            disabled={!actionDone}
            className="flex-1 px-4 py-1.5 rounded-lg text-sm font-semibold transition-[background-color,opacity,transform] duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#6366f1] focus-visible:ring-offset-2 focus-visible:ring-offset-forge-surface active:scale-[0.97]"
            style={{
              background: actionDone ? "#6366f1" : "#2a2d3a",
              color: actionDone ? "#fff" : "#64748b",
              cursor: actionDone ? "pointer" : "not-allowed",
            }}
          >
            {isLast ? `Finish ${CH_RIGHT}` : `Next ${CH_RIGHT}`}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Finale card ───────────────────────────────────────────────────────────────

function FinaleCard({ onDone }: { onDone: () => void }) {
  return (
    <div
      className="fixed bg-forge-surface border rounded-xl animate-fade-in-scale overflow-hidden"
      style={{
        ...TOUR_CARD_STYLE,
        width: FINALE_W,
        top: "20%",
        left: "40%",
        transform: "translate(-50%, -50%)",
        zIndex: 9998,
        pointerEvents: "auto",
      }}
      role="dialog"
      aria-modal="true"
      aria-labelledby="finale-title"
    >
      <div className="h-px bg-gradient-to-r from-transparent via-[#6366f1] to-transparent" />

      <div className="p-7">
        <div className="flex items-center gap-2.5 mb-5">
          <span
            className="text-xl font-bold"
            style={{ color: "#6366f1" }}
            aria-hidden="true"
          >
            {CH_HAMMER}
          </span>
          <h2
            id="finale-title"
            className="text-forge-text text-lg font-bold tracking-tight"
          >
            This is the Forge advantage.
          </h2>
        </div>

        <div className="space-y-3 text-sm leading-relaxed">
          <p className="text-forge-text">
            You just changed one parameter and re-ran your pipeline. Only the
            clustering block and visualization re-executed - everything upstream
            stayed cached. No wasted compute. No waiting.
          </p>
          <p className="text-forge-muted">
            But the bigger win is{" "}
            <span className="text-forge-text font-medium">legibility</span>.
            When your analysis lives as a visual pipeline instead of a notebook
            full of cells, anyone can read the logic at a glance - what feeds
            into what, where the transformation happens, why the output looks
            the way it does.
          </p>
          <p className="text-forge-muted">
            Easier to debug. Easier to hand off. Easier to trust.{" "}
          </p>
          <p>
            <span className="text-forge-text font-medium">
              Data science the way it should be.
            </span>
          </p>
        </div>

        <button
          onClick={onDone}
          className="mt-6 w-full px-4 py-2.5 rounded-lg bg-[#6366f1] hover:bg-[#818cf8] text-white text-sm font-semibold transition-[background-color,transform] duration-150 active:scale-[0.97] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#6366f1] focus-visible:ring-offset-2 focus-visible:ring-offset-forge-surface"
        >
          {`Start building ${CH_RIGHT}`}
        </button>

        <p className="mt-3 text-center text-[11px] text-forge-muted">
          Tip: press <span className="font-mono text-forge-text">Shift+?</span>{" "}
          with nothing selected to reopen this tutorial.
        </p>
      </div>
    </div>
  );
}

// ── Tooltip positioning ────────────────────────────────────────────────────────

function getTooltipStyle(
  rect: SpotRect | null,
  tooltipW: number,
  tooltipH: number,
  noOverlay?: boolean,
  dockSide: "left" | "right" = "right",
): CSSProperties {
  // Free-roam steps: park tooltip away from the workspace the user is editing.
  if (noOverlay) {
    return {
      bottom: 80,
      ...(dockSide === "left"
        ? { left: SCREEN_PAD }
        : { right: SCREEN_PAD }),
    };
  }

  // Centered (no target, has overlay)
  if (!rect) {
    return {
      top: "50%",
      left: "50%",
      transform: "translate(-50%, -50%)",
    };
  }

  // Position relative to spotlight target
  const vw = window.innerWidth;
  const vh = window.innerHeight;

  const spaceRight = vw - (rect.left + rect.width) - SCREEN_PAD;
  const spaceBelow = vh - (rect.top + rect.height) - SCREEN_PAD;
  const spaceAbove = rect.top - SCREEN_PAD;
  const spaceLeft = rect.left - SCREEN_PAD;

  const clampX = (x: number) =>
    Math.max(SCREEN_PAD, Math.min(x, vw - tooltipW - SCREEN_PAD));
  const clampY = (y: number) =>
    Math.max(SCREEN_PAD, Math.min(y, vh - tooltipH - SCREEN_PAD));
  const vCenter = rect.top + rect.height / 2 - tooltipH / 2;

  if (spaceRight >= tooltipW) {
    return { top: clampY(vCenter), left: rect.left + rect.width + SCREEN_PAD };
  }
  if (spaceBelow >= tooltipH) {
    return {
      top: rect.top + rect.height + SCREEN_PAD,
      left: clampX(rect.left),
    };
  }
  if (spaceLeft >= tooltipW) {
    return { top: clampY(vCenter), left: rect.left - tooltipW - SCREEN_PAD };
  }
  if (spaceAbove >= tooltipH) {
    return { top: rect.top - tooltipH - SCREEN_PAD, left: clampX(rect.left) };
  }

  return {
    top: "50%",
    left: "50%",
    transform: "translate(-50%, -50%)",
  };
}
