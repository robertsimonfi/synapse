import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { fetchTrace } from "../api/client";
import type { NodeType, TraceEvent } from "../types";
import { clsx } from "clsx";

// ── Node colour map ────────────────────────────────────────────────────────────

const NODE_STYLE: Record<NodeType, { dot: string; ring: string; label: string }> = {
  AI_REASONING:   { dot: "bg-blue-500",   ring: "ring-blue-700",   label: "AI" },
  ORACLE_WRITE:   { dot: "bg-green-500",  ring: "ring-green-700",  label: "Oracle" },
  DLQ_FAILURE:    { dot: "bg-red-500",    ring: "ring-red-700",    label: "DLQ" },
  OPERATOR_HITL:  { dot: "bg-amber-500",  ring: "ring-amber-700",  label: "Operator" },
  ENGAGEMENT:     { dot: "bg-violet-500", ring: "ring-violet-700", label: "Engagement" },
  ENGAGEMENT_SENT:{ dot: "bg-violet-500", ring: "ring-violet-700", label: "Engagement" },
  CUSTOMER_REPLIED:{ dot: "bg-violet-400", ring: "ring-violet-600", label: "Reply" },
};

function fmtTime(ts: string) {
  if (!ts) return "—";
  return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function EventNode({ event, index }: { event: TraceEvent; index: number }) {
  const [expanded, setExpanded] = useState(false);
  const style = NODE_STYLE[event.node_type] ?? NODE_STYLE.AI_REASONING;

  return (
    <motion.div
      initial={{ opacity: 0, x: -16 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.04 }}
      className="flex gap-4"
    >
      {/* Dot + connector */}
      <div className="flex flex-col items-center">
        <div
          className={clsx(
            "w-3.5 h-3.5 rounded-full ring-2 shrink-0 mt-0.5 cursor-pointer transition-transform hover:scale-125",
            style.dot,
            style.ring,
          )}
          onClick={() => setExpanded((e) => !e)}
        />
        <div className="w-px flex-1 bg-gray-700 mt-1" />
      </div>

      {/* Content */}
      <div className="pb-4 min-w-0 flex-1">
        <div className="flex items-baseline gap-2 flex-wrap">
          <span className="text-xs font-semibold text-gray-400 font-mono">{fmtTime(event.timestamp)}</span>
          <span
            className={clsx(
              "text-[10px] px-1.5 py-0.5 rounded font-bold uppercase tracking-wide",
              {
                "bg-blue-900 text-blue-300":   event.node_type === "AI_REASONING",
                "bg-green-900 text-green-300": event.node_type === "ORACLE_WRITE",
                "bg-red-900 text-red-300":     event.node_type === "DLQ_FAILURE",
                "bg-amber-900 text-amber-300": event.node_type === "OPERATOR_HITL",
                "bg-violet-900 text-violet-300":
                  event.node_type === "ENGAGEMENT" ||
                  event.node_type === "ENGAGEMENT_SENT" ||
                  event.node_type === "CUSTOMER_REPLIED",
              },
            )}
          >
            {style.label}
          </span>
          <span className="text-sm text-gray-200">{event.label}</span>
        </div>

        {expanded && Object.keys(event.detail).length > 0 && (
          <motion.pre
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            className="mt-1.5 text-[11px] text-gray-400 bg-gray-900 rounded p-2 overflow-x-auto max-w-full font-mono"
          >
            {JSON.stringify(
              Object.fromEntries(
                Object.entries(event.detail).filter(([, v]) => v != null),
              ),
              null,
              2,
            )}
          </motion.pre>
        )}
      </div>
    </motion.div>
  );
}

// ── Public component ───────────────────────────────────────────────────────────

interface Props {
  traceId: string | null;
  onClose: () => void;
}

export function Timeline({ traceId, onClose }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["trace", traceId],
    queryFn: () => fetchTrace(traceId!),
    enabled: !!traceId,
    staleTime: 30_000,
  });

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-end">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />

      {/* Drawer */}
      <motion.div
        initial={{ x: "100%" }}
        animate={{ x: 0 }}
        exit={{ x: "100%" }}
        transition={{ type: "spring", stiffness: 280, damping: 30 }}
        className="relative z-10 w-full max-w-xl h-full bg-gray-950 border-l border-gray-800 flex flex-col shadow-2xl"
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-800 shrink-0">
          <div>
            <h2 className="text-base font-semibold text-gray-100">Trace Timeline</h2>
            {traceId && (
              <p className="text-xs font-mono text-gray-500 truncate mt-0.5">{traceId}</p>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-200 transition-colors text-xl leading-none"
          >
            ×
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {isLoading && (
            <div className="flex items-center justify-center h-32 text-gray-500 text-sm animate-pulse">
              Loading trace…
            </div>
          )}
          {error && (
            <div className="text-red-400 text-sm">
              {error instanceof Error ? error.message : "Failed to load trace"}
            </div>
          )}
          {data && (
            <>
              <p className="text-xs text-gray-500 mb-4">
                {data.event_count} events — click a node to expand detail
              </p>
              <div>
                {data.events.map((evt, i) => (
                  <EventNode key={evt.event_id} event={evt} index={i} />
                ))}
                {/* Terminal cap */}
                <div className="flex gap-4">
                  <div className="flex flex-col items-center">
                    <div className="w-3.5 h-3.5 rounded-full bg-gray-600 ring-2 ring-gray-700 shrink-0" />
                  </div>
                  <p className="text-xs text-gray-600 pb-2">End of trace</p>
                </div>
              </div>
            </>
          )}
        </div>
      </motion.div>
    </div>
  );
}
