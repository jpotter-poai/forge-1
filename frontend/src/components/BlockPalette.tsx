import { useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { BlockSpec } from "@/types/pipeline";

interface BlockPaletteProps {
  blocks: BlockSpec[];
  onDragStart: (spec: BlockSpec) => void;
  onCommentDragStart: () => void;
}

const CATEGORY_ORDER = [
  "IO",
  "Operator",
  "Combine",
  "Transform",
  "Statistics",
  "Clustering",
  "Factorization",
  "Dimensionality",
  "Visualization",
  "Special",
];

const CATEGORY_ICON: Record<string, string> = {
  IO: "⇄",
  Operator: "+",
  Combine: "⊕",
  Transform: "Δ",
  Statistics: "σ",
  Clustering: "⊙",
  Factorization: "⊗",
  Dimensionality: "ℝ",
  Visualization: "📈",
  Special: "★",
};

const CATEGORY_COLOR: Record<string, string> = {
  IO: "text-violet-400",
  Operator: "text-green-400",
  Combine: "text-amber-400",
  Transform: "text-sky-400",
  Statistics: "text-blue-400",
  Clustering: "text-emerald-400",
  Visualization: "text-pink-400",
  Factorization: "text-orange-400",
  Dimensionality: "text-teal-400",
  Special: "text-yellow-400",
};

export function BlockPalette({ blocks, onDragStart, onCommentDragStart }: BlockPaletteProps) {
  const [search, setSearch] = useState("");
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());

  const grouped = useMemo(() => {
    const q = search.trim().toLowerCase();
    const map = new Map<string, BlockSpec[]>();
    for (const b of blocks) {
      if (q && !b.name.toLowerCase().includes(q)) continue;
      if (!map.has(b.category)) map.set(b.category, []);
      map.get(b.category)!.push(b);
    }
    // Sort categories
    const ordered = new Map<string, BlockSpec[]>();
    for (const cat of CATEGORY_ORDER) {
      if (map.has(cat)) ordered.set(cat, map.get(cat)!);
    }
    for (const [cat, bs] of map) {
      if (!ordered.has(cat)) ordered.set(cat, bs);
    }
    return ordered;
  }, [blocks, search]);

  const isSearching = search.trim().length > 0;

  function toggleCategory(cat: string) {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  }

  return (
    <aside data-tour="block-palette" className="w-56 flex-shrink-0 bg-forge-surface border-r border-forge-border flex flex-col overflow-hidden">
      <div className="px-4 py-3 border-b border-forge-border">
        <h2 className="text-forge-text font-semibold text-sm tracking-wide uppercase">
          Blocks
        </h2>
        <p className="text-forge-muted text-[11px] mt-0.5">Drag onto canvas</p>
      </div>

      {/* Search bar */}
      <div className="px-2 py-2 border-b border-forge-border">
        <div className="relative" data-tour-block-key="palette-search">
          <span className="absolute left-2 top-1/2 -translate-y-1/2 text-forge-muted text-[11px] pointer-events-none" aria-hidden="true">
            ⌕
          </span>
          <input
            type="text"
            aria-label="Search blocks"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search blocks…"
            className="
              w-full pl-5 pr-6 py-1 rounded-md
              bg-forge-bg border border-forge-border
              text-forge-text text-xs placeholder:text-forge-muted
              focus:outline-none focus:border-forge-accent
              transition-colors duration-100
            "
          />
          {search && (
            <button
              onClick={() => setSearch("")}
              aria-label="Clear search"
              className="absolute right-2 top-1/2 -translate-y-1/2 text-forge-muted hover:text-forge-text text-[11px] leading-none"
            >
              ✕
            </button>
          )}
        </div>
      </div>

      <div className="overflow-y-auto flex-1 px-2 py-3 space-y-4">
        {blocks.length === 0 && (
          <p className="text-forge-muted text-xs px-2 py-4 text-center animate-pulse">
            Loading blocks…
          </p>
        )}
        {grouped.size === 0 && blocks.length > 0 && (
          <p className="text-forge-muted text-xs px-2 py-4 text-center animate-fade-in">
            No blocks match "{search}"
          </p>
        )}
        {Array.from(grouped.entries()).map(([category, specs]) => {
          const isCollapsed = !isSearching && collapsed.has(category);
          return (
            <div key={category} data-tour-category={category}>
              <button
                onClick={() => toggleCategory(category)}
                className={`w-full flex items-center gap-1.5 px-2 py-1.5 text-[11px] font-semibold uppercase tracking-wider rounded hover:bg-forge-border/30 transition-colors duration-100 ${CATEGORY_COLOR[category] ?? "text-forge-muted"}`}
              >
                <span aria-hidden="true">{CATEGORY_ICON[category] ?? "•"}</span>
                <span className="flex-1 text-left">{category}</span>
                <span className={`text-forge-muted transition-transform duration-200 ${isCollapsed ? "-rotate-90" : ""}`}>
                  ▾
                </span>
              </button>
              {!isCollapsed && (
                <div className="space-y-1 mt-1">
                  {specs.map((spec, i) => (
                    <PaletteBlock key={spec.key} spec={spec} index={i} onDragStart={onDragStart} />
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Annotations section */}
      <div className="px-2 py-3 border-t border-forge-border flex-shrink-0">
        <div className="flex items-center gap-1.5 px-2 py-1.5 text-[11px] font-semibold uppercase tracking-wider text-forge-muted">
          <span aria-hidden="true">◻</span>
          <span>Annotations</span>
        </div>
        <div
          draggable
          onDragStart={onCommentDragStart}
          className="
            mx-1 mt-1 px-3 py-2 rounded-md
            bg-forge-bg border border-dashed border-forge-border/70
            text-forge-muted text-xs
            cursor-grab active:cursor-grabbing
            hover:border-forge-muted/60 hover:bg-forge-border/20
            hover:shadow-sm hover:shadow-forge-border/30
            transition-[colors,box-shadow] duration-150
            select-none
          "
          title="Drag onto canvas to add a comment annotation"
        >
          <div className="font-medium">Comment</div>
          <div className="text-[10px] text-forge-muted mt-0.5">Annotation block</div>
        </div>
      </div>
    </aside>
  );
}

// ── Palette block with hover tooltip ─────────────────────────────────────────

function PaletteBlock({
  spec,
  index,
  onDragStart,
}: {
  spec: BlockSpec;
  index: number;
  onDragStart: (spec: BlockSpec) => void;
}) {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPos, setTooltipPos] = useState<{ top: number; left: number } | null>(null);
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const blockRef = useRef<HTMLDivElement>(null);

  const handleMouseEnter = () => {
    if (!spec.description) return;
    hoverTimer.current = setTimeout(() => {
      if (blockRef.current) {
        const rect = blockRef.current.getBoundingClientRect();
        setTooltipPos({
          top: rect.top,
          left: rect.right + 8,
        });
      }
      setShowTooltip(true);
    }, 400);
  };

  const handleMouseLeave = () => {
    if (hoverTimer.current) clearTimeout(hoverTimer.current);
    setShowTooltip(false);
  };

  return (
    <>
      <div
        ref={blockRef}
        data-tour-block-key={spec.key}
        draggable
        onDragStart={() => {
          if (hoverTimer.current) clearTimeout(hoverTimer.current);
          setShowTooltip(false);
          onDragStart(spec);
        }}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        className="
          mx-1 px-3 py-2 rounded-md
          bg-forge-bg border border-forge-border
          text-forge-text text-xs
          cursor-grab active:cursor-grabbing
          hover:border-forge-accent hover:bg-forge-accent/10
          hover:shadow-sm hover:shadow-forge-accent/10
          active:shadow-md active:shadow-forge-accent/20
          transition-[colors,box-shadow,transform] duration-150
          select-none
        "
        style={{ animationDelay: `${index * 30}ms` }}
      >
        <div className="font-medium truncate">{spec.name}</div>
        {spec.n_inputs === 0 && (
          <div className="text-[10px] text-forge-muted mt-0.5">Source</div>
        )}
        {spec.n_inputs > 1 && (
          <div className="text-[10px] text-forge-muted mt-0.5">
            {spec.n_inputs} inputs
          </div>
        )}
      </div>

      {/* Hover tooltip — rendered via portal so it escapes overflow containers */}
      {showTooltip && spec.description && tooltipPos &&
        createPortal(
          <div
            role="tooltip"
            className="
              fixed z-[9999]
              w-52 px-3 py-2 rounded-md
              bg-forge-bg border border-forge-border
              shadow-lg shadow-black/40
              text-forge-text text-[11px] leading-snug
              pointer-events-none animate-fade-in
            "
            style={{ top: tooltipPos.top, left: tooltipPos.left }}
          >
            <div className="font-semibold text-xs mb-1">{spec.name}</div>
            <p className="text-forge-muted">{spec.description}</p>
            <div className="mt-1.5 flex items-center gap-2 text-[10px] text-forge-muted/70">
              <span>v{spec.version}</span>
              {spec.n_inputs > 0 && <span>· {spec.n_inputs} input{spec.n_inputs > 1 ? "s" : ""}</span>}
            </div>
          </div>,
          document.body,
        )}
    </>
  );
}
