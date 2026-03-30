import { memo, useCallback } from "react";
import {
  NodeResizer,
  useReactFlow,
  type NodeProps,
  type ResizeParams,
} from "@xyflow/react";
import type { CommentNodeData } from "@/hooks/usePipeline";
import { useBackdropBlur } from "@/hooks/useBackdropBlur";

export const CommentNode = memo(function CommentNode({
  id,
  data,
  selected,
}: NodeProps) {
  const { title, description } = data as CommentNodeData;
  const { setNodes } = useReactFlow();
  const backdropBlur = useBackdropBlur();

  const updateField = useCallback(
    (field: "title" | "description", value: string) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id ? { ...n, data: { ...n.data, [field]: value } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const handleResize = useCallback(
    (_: unknown, params: ResizeParams) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id
            ? {
                ...n,
                // params.x/y are the new absolute flow-space position —
                // required when dragging top / left / top-left handles.
                position: { x: params.x, y: params.y },
                style: {
                  ...n.style,
                  width: params.width,
                  height: params.height,
                },
              }
            : n,
        ),
      );
    },
    [id, setNodes],
  );

  return (
    <div className="w-full h-full relative">
      <NodeResizer
        isVisible={selected}
        minWidth={150}
        minHeight={80}
        onResize={handleResize}
        color="#64748b"
        handleStyle={{
          width: 9,
          height: 9,
          borderRadius: 2,
          background: "#2a2d3a",
          border: "1.5px solid #64748b",
        }}
        lineStyle={{
          borderColor: "#64748b",
          borderWidth: 1,
          borderStyle: "dashed",
        }}
      />

      {/* Outer rounded rect — the visual "block" */}
      <div
        className={`
          w-full h-full rounded-2xl flex flex-col overflow-hidden
          border-2 transition-colors duration-100
          ${selected
            ? "bg-forge-border/30 border-solid border-forge-muted/70"
            : "bg-forge-surface/20 border-dashed border-forge-border/50"}
        `}
        style={{
          ...(backdropBlur ? { backdropFilter: "blur(2px)" } : {}),
          ...(selected ? { boxShadow: "0 0 0 1px rgba(100,116,139,0.25), 0 4px 16px rgba(0,0,0,0.4)" } : {}),
        }}
      >
        {/* Title row */}
        <div className="px-4 pt-3 pb-1 flex-shrink-0">
          <input
            value={title}
            onChange={(e) => updateField("title", e.target.value)}
            onMouseDown={(e) => e.stopPropagation()}
            autoCapitalize="off"
            autoCorrect="off"
            spellCheck={false}
            placeholder="Comment Title"
            className="
              nodrag nopan
              w-full bg-transparent
              text-forge-text font-semibold text-sm
              outline-none border-none
              placeholder:text-forge-border
              cursor-text
            "
          />
        </div>

        {/* Thin separator */}
        <div className="mx-4 h-px bg-forge-border/40 flex-shrink-0" />

        {/* Description area */}
        <div className="px-4 pt-2 pb-3 flex-1 min-h-0">
          <textarea
            value={description}
            onChange={(e) => updateField("description", e.target.value)}
            onMouseDown={(e) => e.stopPropagation()}
            autoCapitalize="off"
            autoCorrect="off"
            spellCheck={false}
            placeholder="Add a description…"
            className="
              nodrag nopan
              w-full h-full
              bg-transparent
              text-forge-muted text-xs
              resize-none outline-none border-none
              placeholder:text-forge-border
              cursor-text leading-relaxed
            "
          />
        </div>
      </div>
    </div>
  );
});
