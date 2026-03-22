import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  type Connection,
  type OnNodesChange,
  type OnEdgesChange,
  type Node,
  type Edge,
  type ReactFlowInstance,
} from "@xyflow/react";
import { exportPipelinePng, exportPipelinePdf } from "./utils/exportCanvas";
import { downloadPipelineExport } from "./api/client";
import { BlockPalette } from "./components/BlockPalette";
import { Canvas } from "./components/Canvas";
import { NodeInspector } from "./components/NodeInspector";
import { OnboardingTour } from "./components/OnboardingTour";
import { OnboardingWelcome } from "./components/OnboardingWelcome";
import { Toolbar } from "./components/Toolbar";
import { usePipeline, type ForgeNodeData } from "./hooks/usePipeline";
import type { BlockSpec } from "./types/pipeline";

const HISTORY_LIMIT = 50;

interface GraphSnapshot {
  nodes: Node<ForgeNodeData>[];
  edges: Edge[];
}

function graphSignature(nodes: Node<ForgeNodeData>[], edges: Edge[]): string {
  const nodeIds = nodes.map((n) => n.id).sort().join("|");
  const edgeIds = edges.map((e) => e.id).sort().join("|");
  return `${nodeIds}::${edgeIds}`;
}

function cloneValue<T>(value: T): T {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value)) as T;
}

function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;
  if (target.closest("[contenteditable='true']")) return true;
  const tag = target.tagName.toLowerCase();
  return tag === "input" || tag === "textarea" || tag === "select";
}

/** Returns true if the user has a non-empty text selection in the document. */
function hasTextSelection(): boolean {
  const sel = window.getSelection();
  return !!sel && sel.type === "Range" && sel.toString().length > 0;
}

// ── Cross-tab clipboard via localStorage ────────────────────────────────────
const FORGE_CLIPBOARD_KEY = "forge-clipboard-v1";

function writeClipboard(nodes: Node<ForgeNodeData>[]) {
  try {
    localStorage.setItem(FORGE_CLIPBOARD_KEY, JSON.stringify(nodes));
  } catch {
    // localStorage full or unavailable — silent fail, in-memory clipboard still works
  }
}

function readClipboard(): Node<ForgeNodeData>[] | null {
  try {
    const raw = localStorage.getItem(FORGE_CLIPBOARD_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as Node<ForgeNodeData>[];
  } catch {
    return null;
  }
}

export default function App() {
  const {
    blocks,
    nodes,
    setNodes,
    edges,
    setEdges,
    pipelineId,
    pipelineName,
    setPipelineName,
    selectedNodeId,
    setSelectedNodeId,
    isRunning,
    isStopping,
    isDirty,
    runError,
    addNode,
    addComment,
    deleteNode,
    pasteNodes,
    updateNodeParams,
    newPipelineDraft,
    runPipeline,
    stopPipeline,
    savePipeline,
    prettifyPipeline,
    loadPipeline,
  } = usePipeline();

  // ── Onboarding ──────────────────────────────────────────────────────────────

  const [showWelcome, setShowWelcome] = useState(() => {
    try {
      return !localStorage.getItem("forge-onboarded");
    } catch {
      return false;
    }
  });
  const [showTour, setShowTour] = useState(false);
  const [runCount, setRunCount] = useState(0);
  const [completedRunCount, setCompletedRunCount] = useState(0);
  const [paramsChangeCount, setParamsChangeCount] = useState(0);
  const [showReplayTourToast, setShowReplayTourToast] = useState(false);
  const [showTutorialHintToast, setShowTutorialHintToast] = useState(false);
  const tutorialHintTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const prevIsRunningRef = useRef(false);

  const handleStartTour = useCallback(() => {
    setShowWelcome(false);
    setShowTour(true);
  }, []);

  const flashTutorialHintToast = useCallback(() => {
    setShowTutorialHintToast(true);
    if (tutorialHintTimerRef.current) {
      window.clearTimeout(tutorialHintTimerRef.current);
    }
    tutorialHintTimerRef.current = window.setTimeout(() => {
      setShowTutorialHintToast(false);
      tutorialHintTimerRef.current = null;
    }, 5000);
  }, []);

  const handleReplayTour = useCallback(() => {
    setShowReplayTourToast(false);
    setShowWelcome(true);
    setShowTour(false);
    setRunCount(0);
    setCompletedRunCount(0);
    setParamsChangeCount(0);
  }, []);

  const handleSkipOnboarding = useCallback(() => {
    setShowWelcome(false);
    flashTutorialHintToast();
    try {
      localStorage.setItem("forge-onboarded", "1");
    } catch { /* localStorage unavailable */ }
  }, [flashTutorialHintToast]);

  const handleSkipTour = useCallback(() => {
    setShowTour(false);
    flashTutorialHintToast();
    try {
      localStorage.setItem("forge-onboarded", "1");
    } catch { /* localStorage unavailable */ }
  }, [flashTutorialHintToast]);

  const handleTourDone = useCallback(() => {
    setShowTour(false);
    setShowReplayTourToast(false);
    try {
      localStorage.setItem("forge-onboarded", "1");
    } catch { /* localStorage unavailable */ }
  }, []);

  useEffect(() => {
    if (showWelcome || showTour) {
      setShowReplayTourToast(false);
    }
  }, [showTour, showWelcome]);

  useEffect(() => {
    return () => {
      if (tutorialHintTimerRef.current) {
        window.clearTimeout(tutorialHintTimerRef.current);
      }
    };
  }, []);

  // Wrapped run handler — tracks run count for the onboarding tour
  const handleRunPipeline = useCallback(() => {
    runPipeline();
    setRunCount((c) => c + 1);
  }, [runPipeline]);

  useEffect(() => {
    if (prevIsRunningRef.current && !isRunning && !runError) {
      setCompletedRunCount((count) => count + 1);
    }
    prevIsRunningRef.current = isRunning;
  }, [isRunning, runError]);

  // Wrapped params handler — tracks edits for the onboarding tour
  const handleUpdateNodeParams = useCallback(
    (nodeId: string, params: Record<string, unknown>) => {
      updateNodeParams(nodeId, params);
      setParamsChangeCount((c) => c + 1);
    },
    [updateNodeParams],
  );

  // Canvas refs for export
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rfInstanceRef = useRef<ReactFlowInstance<any, any> | null>(null);
  const canvasWrapperRef = useRef<HTMLDivElement | null>(null);
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const showExportError = useCallback((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Export failed", error);
    setExportError(message);
    setTimeout(() => setExportError(null), 5000);
  }, []);

  const handleExportPng = useCallback(async () => {
    if (!rfInstanceRef.current || !canvasWrapperRef.current || isExporting) return;
    setIsExporting(true);
    try {
      await exportPipelinePng(rfInstanceRef.current, canvasWrapperRef.current, pipelineName);
    } catch (error: unknown) {
      showExportError(error);
    } finally {
      setIsExporting(false);
    }
  }, [pipelineName, isExporting, showExportError]);

  const handleExportPdf = useCallback(async () => {
    if (!rfInstanceRef.current || !canvasWrapperRef.current || isExporting) return;
    setIsExporting(true);
    try {
      await exportPipelinePdf(rfInstanceRef.current, canvasWrapperRef.current, pipelineName);
    } catch (error: unknown) {
      showExportError(error);
    } finally {
      setIsExporting(false);
    }
  }, [pipelineName, isExporting, showExportError]);

  const handleExportBundle = useCallback(
    async (format: "python" | "notebook") => {
      if (isExporting) return;
      setIsExporting(true);
      try {
        const savedPipelineId = await savePipeline();
        if (!savedPipelineId) {
          throw new Error("Pipeline save did not return an id.");
        }
        await downloadPipelineExport(savedPipelineId, format);
      } catch (error: unknown) {
        showExportError(error);
      } finally {
        setIsExporting(false);
      }
    },
    [isExporting, savePipeline, showExportError],
  );

  // Track which block spec is being dragged from the palette
  const [draggingSpec, setDraggingSpec] = useState<BlockSpec | null>(null);
  const [draggingComment, setDraggingComment] = useState(false);
  const historyRef = useRef<GraphSnapshot[]>([]);
  const lastHistorySignatureRef = useRef<string | null>(null);
  const isApplyingUndoRef = useRef(false);
  const clipboardRef = useRef<Node<ForgeNodeData>[]>([]);
  const pasteDepthRef = useRef(0);

  const clearHistory = useCallback(() => {
    historyRef.current = [];
    lastHistorySignatureRef.current = null;
  }, []);

  const pushHistorySnapshot = useCallback(() => {
    if (isApplyingUndoRef.current) return;
    const signature = graphSignature(nodes, edges);
    if (lastHistorySignatureRef.current === signature) return;
    historyRef.current.push(cloneValue({ nodes, edges }));
    lastHistorySignatureRef.current = signature;
    if (historyRef.current.length > HISTORY_LIMIT) {
      historyRef.current.splice(0, historyRef.current.length - HISTORY_LIMIT);
    }
  }, [nodes, edges]);

  const undoLastStructuralChange = useCallback(() => {
    const previous = historyRef.current.pop();
    if (!previous) return;
    const top =
      historyRef.current.length > 0
        ? historyRef.current[historyRef.current.length - 1]
        : undefined;
    lastHistorySignatureRef.current = top
      ? graphSignature(top.nodes, top.edges)
      : null;
    isApplyingUndoRef.current = true;
    setNodes(previous.nodes);
    setEdges(previous.edges);
    setSelectedNodeId(null);
    window.setTimeout(() => {
      isApplyingUndoRef.current = false;
    }, 0);
  }, [setNodes, setEdges, setSelectedNodeId]);

  // ── React Flow change handlers ──────────────────────────────────────────────

  const onNodesChange: OnNodesChange<Node<ForgeNodeData>> = useCallback(
    (changes) => {
      const removedNodeIds = changes
        .filter((c) => c.type === "remove")
        .map((c) => c.id);
      if (removedNodeIds.length > 0 && !isApplyingUndoRef.current) {
        pushHistorySnapshot();
        const removedIdSet = new Set(removedNodeIds);
        if (selectedNodeId && removedIdSet.has(selectedNodeId)) {
          setSelectedNodeId(null);
        }
      }
      setNodes((ns) => applyNodeChanges(changes, ns));
    },
    [pushHistorySnapshot, selectedNodeId, setNodes, setSelectedNodeId],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => {
      const hasEdgeRemoval = changes.some((c) => c.type === "remove");
      if (hasEdgeRemoval && !isApplyingUndoRef.current) {
        pushHistorySnapshot();
      }
      setEdges((es) => applyEdgeChanges(changes, es));
    },
    [pushHistorySnapshot, setEdges],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      pushHistorySnapshot();
      setEdges((es) =>
        addEdge(
          {
            ...connection,
            id: `e_${connection.source}_${connection.target}_${Date.now()}`,
            type: "smoothstep",
            animated: false,
          },
          es,
        ),
      );
    },
    [pushHistorySnapshot, setEdges],
  );

  // ── Drop from palette ───────────────────────────────────────────────────────

  const handleDropBlock = useCallback(
    (spec: BlockSpec, position: { x: number; y: number }) => {
      pushHistorySnapshot();
      const id = addNode(spec, position);
      setSelectedNodeId(id);
      setDraggingSpec(null);
    },
    [addNode, pushHistorySnapshot, setSelectedNodeId],
  );

  const handleDropComment = useCallback(
    (position: { x: number; y: number }) => {
      pushHistorySnapshot();
      addComment(position);
      setDraggingComment(false);
    },
    [addComment, pushHistorySnapshot],
  );

  const handleDeleteNode = useCallback(
    (nodeId: string) => {
      pushHistorySnapshot();
      deleteNode(nodeId);
    },
    [deleteNode, pushHistorySnapshot],
  );

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (isEditableTarget(event.target)) return;

      if (
        event.key === "?" &&
        event.shiftKey &&
        !event.ctrlKey &&
        !event.metaKey &&
        !event.altKey
      ) {
        const hasSelectedNodes = nodes.some((node) => node.selected);
        if (!showWelcome && !showTour && !selectedNodeId && !hasSelectedNodes) {
          event.preventDefault();
          setShowReplayTourToast(true);
        }
        return;
      }

      const isMod = event.ctrlKey || event.metaKey;
      if (!isMod || event.altKey) return;

      const key = event.key.toLowerCase();

      if (key === "c" && !event.shiftKey) {
        // If the user has text selected, let the browser's native copy work
        if (hasTextSelection()) return;

        const selected = nodes.filter((n) => n.selected);
        const fallback =
          selected.length === 0 && selectedNodeId
            ? nodes.filter((n) => n.id === selectedNodeId)
            : [];
        const toCopy = selected.length > 0 ? selected : fallback;
        if (toCopy.length === 0) return;

        event.preventDefault();
        const cloned = cloneValue(toCopy);
        clipboardRef.current = cloned;
        pasteDepthRef.current = 0;
        // Write to cross-tab clipboard
        writeClipboard(cloned);
        return;
      }

      if (key === "v" && !event.shiftKey) {
        // Try in-memory clipboard first, fall back to cross-tab
        let source = clipboardRef.current;
        if (source.length === 0) {
          const crossTab = readClipboard();
          if (crossTab && crossTab.length > 0) {
            source = crossTab;
            clipboardRef.current = source;
          }
        }
        if (source.length === 0) return;
        event.preventDefault();
        pushHistorySnapshot();
        pasteDepthRef.current += 1;
        const offset = 40 * pasteDepthRef.current;
        const pastedIds = pasteNodes(source, { x: offset, y: offset });
        if (
          source.length === 1 &&
          source[0].type !== "commentBlock"
        ) {
          setSelectedNodeId(pastedIds[0] ?? null);
        } else {
          setSelectedNodeId(null);
        }
        return;
      }

      if (key === "z" && !event.shiftKey) {
        event.preventDefault();
        undoLastStructuralChange();
      }
    };

    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [
    nodes,
    selectedNodeId,
    pasteNodes,
    pushHistorySnapshot,
    setSelectedNodeId,
    showTour,
    showWelcome,
    undoLastStructuralChange,
  ]);

  // ── Derived selected node ───────────────────────────────────────────────────
  // Exclude comment nodes — they use inline editing, no inspector panel needed.

  const selectedNode = selectedNodeId
    ? nodes.find((n) => n.id === selectedNodeId && n.type !== "commentBlock")
    : null;

  // ── Comment z-index: larger area sits further back ──────────────────────────
  // Sort comment nodes by area descending; assign zIndex -(N) … -1 so the
  // largest comment is behind the smallest, all still behind regular blocks.

  const nodesWithCommentZIndex = useMemo(() => {
    const commentNodes = nodes.filter((n) => n.type === "commentBlock");
    // Sort largest → smallest area
    const sorted = [...commentNodes].sort((a, b) => {
      const aArea = (Number(a.style?.width) || 0) * (Number(a.style?.height) || 0);
      const bArea = (Number(b.style?.width) || 0) * (Number(b.style?.height) || 0);
      return bArea - aArea;
    });
    const zMap = new Map<string, number>();
    const n = sorted.length;
    sorted.forEach((node, i) => zMap.set(node.id, -(n - i)));

    return nodes.map((node) => {
      if (node.type !== "commentBlock") return node;
      const zi = zMap.get(node.id) ?? -1;
      return node.zIndex === zi ? node : { ...node, zIndex: zi };
    });
  }, [nodes]);

  // ── New pipeline ────────────────────────────────────────────────────────────

  const handleNewPipeline = useCallback(() => {
    if (isDirty && !confirm("Create a new pipeline? Unsaved changes will be lost.")) {
      return;
    }
    newPipelineDraft();
    clearHistory();
  }, [clearHistory, isDirty, newPipelineDraft]);

  const handleLoadPipeline = useCallback(
    async (id: string) => {
      await loadPipeline(id);
      clearHistory();
      setSelectedNodeId(null);
    },
    [clearHistory, loadPipeline, setSelectedNodeId],
  );

  return (
    <div className="h-screen w-screen flex flex-col bg-forge-bg text-forge-text overflow-hidden">
      <Toolbar
        pipelineName={pipelineName}
        pipelineId={pipelineId}
        isRunning={isRunning}
        isStopping={isStopping}
        isDirty={isDirty}
        runError={exportError ?? runError}
        onNameChange={setPipelineName}
        onSave={savePipeline}
        onPrettify={prettifyPipeline}
        onLoad={handleLoadPipeline}
        onRun={handleRunPipeline}
        onStop={stopPipeline}
        onNewPipeline={handleNewPipeline}
        isExporting={isExporting}
        onExportPng={handleExportPng}
        onExportPdf={handleExportPdf}
        onExportPython={() => {
          void handleExportBundle("python");
        }}
        onExportNotebook={() => {
          void handleExportBundle("notebook");
        }}
      />

      <div className="flex flex-1 overflow-hidden">
        <BlockPalette
          blocks={blocks}
          onDragStart={(spec) => {
            setDraggingSpec(spec);
            setDraggingComment(false);
          }}
          onCommentDragStart={() => {
            setDraggingComment(true);
            setDraggingSpec(null);
          }}
        />

        <Canvas
          nodes={nodesWithCommentZIndex}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={(id) =>
            setSelectedNodeId((prev) => (prev === id ? null : id))
          }
          onPaneClick={() => setSelectedNodeId(null)}
          onSelectionChange={(selectedNodes) => {
            const regular = selectedNodes.filter((n) => n.type !== "commentBlock");
            setSelectedNodeId(regular.length === 1 ? regular[0].id : null);
          }}
          onDropBlock={handleDropBlock}
          onDropComment={handleDropComment}
          draggingSpec={draggingSpec}
          draggingComment={draggingComment}
          onCanvasReady={(instance, wrapper) => {
            rfInstanceRef.current = instance;
            canvasWrapperRef.current = wrapper;
          }}
        />

        {selectedNode && (
          <NodeInspector
            node={selectedNode as Node<ForgeNodeData>}
            onParamsChange={handleUpdateNodeParams}
            onDelete={handleDeleteNode}
          />
        )}
      </div>

      {showWelcome && (
        <OnboardingWelcome onStartTour={handleStartTour} onSkip={handleSkipOnboarding} />
      )}
      {showTour && (
        <OnboardingTour
          onDone={handleTourDone}
          onSkip={handleSkipTour}
          nodes={nodesWithCommentZIndex}
          edges={edges}
          runCount={runCount}
          completedRunCount={completedRunCount}
          paramsChangeCount={paramsChangeCount}
          isDragging={draggingSpec !== null || draggingComment}
        />
      )}
      {showTutorialHintToast && <TutorialHintToast />}
      {showReplayTourToast && (
        <ReplayTourToast
          onConfirm={handleReplayTour}
          onDismiss={() => setShowReplayTourToast(false)}
        />
      )}
    </div>
  );
}

function ReplayTourToast({
  onConfirm,
  onDismiss,
}: {
  onConfirm: () => void;
  onDismiss: () => void;
}) {
  return (
    <div className="fixed bottom-5 left-5 z-50 w-[360px] max-w-[calc(100vw-2.5rem)] rounded-xl border border-[#818cf8]/60 bg-forge-surface/95 px-4 py-3 shadow-[0_0_0_1px_rgba(129,140,248,0.22),0_0_24px_rgba(99,102,241,0.22),0_16px_36px_rgba(0,0,0,0.45)] backdrop-blur-sm animate-fade-in-scale">
      <p className="text-sm text-forge-text">
        Do you want to see the tutorial again?
      </p>
      <div className="mt-3 flex gap-2">
        <button
          onClick={onConfirm}
          className="px-3 py-1.5 rounded-lg bg-[#6366f1] hover:bg-[#818cf8] text-white text-sm font-medium transition-[background-color,transform] duration-150 active:scale-[0.98]"
        >
          Yes
        </button>
        <button
          onClick={onDismiss}
          className="px-3 py-1.5 rounded-lg border border-forge-border text-forge-muted hover:text-forge-text hover:border-forge-border-mid text-sm transition-colors duration-150"
        >
          Dismiss
        </button>
      </div>
    </div>
  );
}

function TutorialHintToast() {
  return (
    <div className="fixed bottom-5 left-5 z-40 max-w-[calc(100vw-2.5rem)] rounded-lg border border-forge-border bg-forge-surface/95 px-3 py-2 shadow-lg shadow-black/35 backdrop-blur-sm animate-fade-in">
      <p className="text-xs text-forge-muted">
        Didn&apos;t mean to do that?{" "}
        <span className="text-forge-text">Shift + ?</span> to bring the tutorial
        back.
      </p>
    </div>
  );
}
