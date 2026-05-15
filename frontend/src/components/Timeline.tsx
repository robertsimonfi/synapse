import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import { fetchTrace } from "../api/client";
import type { NodeType, TraceEvent } from "../types";
import { clsx } from "clsx";

const NODE_STYLE: Record<NodeType, { dot: string; ring: string; label: string }> = {
  AI_REASONING:      { dot: "bg-blue-500",    ring: "ring-blue-700",    label: "AI" },
  ORACLE_WRITE:      { dot: "bg-emerald-500", ring: "ring-emerald-700", label: "Oracle" },
  DLQ_FAILURE:       { dot: "bg-red-500",     ring: "ring-red-700",     label: "DLQ" },
  OPERATOR_HITL:     { dot: "bg-amber-500",   ring: "ring-amber-700",   label: "Operator" },
  OPERATOR_FEEDBACK: { dot: "bg-pink-500",    ring: "ring-pink-700",    label: "Feedback" },
  ENGAGEMENT:        { dot: "bg-violet-500",  ring: "ring-violet-700",  label: "Engagement" },
  ENGAGEMENT_SENT:   { dot: "bg-violet-500",  ring: "ring-violet-700",  label: "Engagement" },
  CUSTOMER_REPLIED:  { dot: "bg-violet-400",  ring: "ring-violet-600",  label: "Reply" },
};

const NODE_BADGE: Partial<Record<NodeType, string>> = {
  AI_REASONING:      "bg-blue-500/12 text-blue-300 border border-blue-500/25",
  ORACLE_WRITE:      "bg-emerald-500/12 text-emerald-300 border border-emerald-500/25",
  DLQ_FAILURE:       "bg-red-500/12 text-red-300 border border-red-500/25",
  OPERATOR_HITL:     "bg-amber-500/12 text-amber-300 border border-amber-500/25",
  OPERATOR_FEEDBACK: "bg-pink-500/12 text-pink-300 border border-pink-500/25",
  ENGAGEMENT:        "bg-violet-500/12 text-violet-300 border border-violet-500/25",
  ENGAGEMENT_SENT:   "bg-violet-500/12 text-violet-300 border border-violet-500/25",
  CUSTOMER_REPLIED:  "bg-violet-400/12 text-violet-200 border border-violet-400/25",
};

function fmtTime(ts: string) {
  if (!ts) return "—";
  return new Date(ts).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function EventNode({ event, index }: { event: TraceEvent; index: number }) {
  const [expanded, setExpanded] = useState(false);
  const style = NODE_STYLE[event.node_type] ?? NODE_STYLE.AI_REASONING;
  const badgeClass = NODE_BADGE[event.node_type] ?? "bg-white/6 text-gray-400 border border-white/10";

  return (
    <motion.div
      initial={{ opacity: 0, x: -18 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.04, duration: 0.32, ease: "easeOut" }}
      className="flex gap-4"
    >
      {/* Dot + connector */}
      <div className="flex flex-col items-center">
        <button
          onClick={() => setExpanded((e) => !e)}
          className={clsx(
            "w-3.5 h-3.5 rounded-full ring-2 shrink-0 mt-0.5 transition-all duration-200",
            "hover:scale-125 hover:brightness-125",
            style.dot,
            style.ring,
          )}
        />
        <div className="w-px flex-1 bg-white/6 mt-1" />
      </div>

      {/* Content */}
      <div className="pb-4 min-w-0 flex-1">
        <div className="flex items-baseline gap-2 flex-wrap">
          <span className="text-xs text-gray-600 font-mono">{fmtTime(event.timestamp)}</span>
          <span className={clsx("text-[10px] px-1.5 py-0.5 rounded font-bold uppercase tracking-wider", badgeClass)}>
            {style.label}
          </span>
          <span className="text-sm text-gray-200">{event.label}</span>
        </div>

        <AnimatePresence>
          {expanded && Object.keys(event.detail).length > 0 && (
            <motion.pre
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: 0.2 }}
              className="mt-2 text-[11px] text-gray-400 bg-black/35 rounded-xl p-2.5 overflow-x-auto max-w-full font-mono border border-white/5"
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
        </AnimatePresence>
      </div>
    </motion.div>
  );
}

// ── Public component ─────────────────────────────────────────────────────

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
      {/* Animated backdrop */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.25 }}
        className="absolute inset-0 bg-black/55 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Drawer */}
      <motion.div
        initial={{ x: "100%", opacity: 0.6 }}
        animate={{ x: 0, opacity: 1 }}
        exit={{ x: "100%", opacity: 0 }}
        transition={{ type: "spring", stiffness: 300, damping: 32 }}
        className="relative z-10 w-full max-w-xl h-full glass-dark border-l border-white/6 flex flex-col shadow-2xl"
      >
        {/* Drawer header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-white/5 shrink-0">
          <div>
            <h2 className="text-sm font-semibold gradient-text">Trace Timeline</h2>
            {traceId && (
              <p className="text-[11px] font-mono text-gray-600 truncate mt-0.5">{traceId}</p>
            )}
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg bg-white/5 hover:bg-white/10 text-gray-500 hover:text-gray-200 transition-all flex items-center justify-center text-lg leading-none"
          >
            ×
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {isLoading && (
            <div className="space-y-3 mt-2">
              {[80, 60, 90, 50, 70].map((w, i) => (
                <div key={i} className="flex gap-4 items-start">
                  <div className="skeleton w-3.5 h-3.5 rounded-full mt-0.5 shrink-0" />
                  <div className={`skeleton h-4`} style={{ width: `${w}%` }} />
                </div>
              ))}
            </div>
          )}
          {error && (
            <div className="text-red-400/80 text-sm mt-4">
              {error instanceof Error ? error.message : "Failed to load trace"}
            </div>
          )}
          {data && (
            <>
              <p className="text-[11px] text-gray-600 mb-4">
                {data.event_count} events — click a node to expand detail
              </p>
              <div>
                {data.events.map((evt, i) => (
                  <EventNode key={evt.event_id} event={evt} index={i} />
                ))}
                {/* Terminal cap */}
                <div className="flex gap-4">
                  <div className="flex flex-col items-center">
                    <div className="w-3.5 h-3.5 rounded-full bg-white/10 ring-2 ring-white/6 shrink-0" />
                  </div>
                  <p className="text-xs text-gray-700 pb-2">End of trace</p>
                </div>
              </div>
            </>
          )}
        </div>
      </motion.div>
    </div>
  );
}
