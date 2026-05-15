import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import toast from "react-hot-toast";
import { clsx } from "clsx";
import { fetchDlqEvents, acknowledgeDlqEvent } from "../api/client";
import type { DlqEvent } from "../types";

function fmtDate(iso: string) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function DlqRow({ event, operatorId }: { event: DlqEvent; operatorId: string }) {
  const [expanded, setExpanded] = useState(false);
  const qc = useQueryClient();

  const ackMutation = useMutation({
    mutationFn: () => acknowledgeDlqEvent(event.outbox_id, operatorId),
    onSuccess: () => {
      toast.success(`DLQ event acknowledged: ${event.outbox_id.slice(0, 8)}…`);
      qc.invalidateQueries({ queryKey: ["dlq"] });
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const isAck = event.status === "ACKNOWLEDGED";

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.97 }}
      className={clsx(
        "rounded-2xl glass border p-4 flex flex-col gap-3",
        isAck
          ? "border-white/6 opacity-55"
          : "border-red-500/30 shadow-sm shadow-red-900/20",
      )}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-8 h-8 rounded-xl bg-red-500/10 border border-red-500/25 flex items-center justify-center text-base shrink-0">
            ⚠
          </div>
          <div className="min-w-0">
            <p className="font-semibold text-sm text-gray-100 truncate">
              {event.action_type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
            </p>
            <p className="text-[11px] text-gray-600 font-mono truncate">
              {event.outbox_id}
            </p>
          </div>
        </div>
        <span
          className={clsx(
            "text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wider shrink-0",
            isAck
              ? "bg-white/5 text-gray-500 border border-white/8"
              : "bg-red-500/12 text-red-300 border border-red-500/25",
          )}
        >
          {event.status}
        </span>
      </div>

      {/* Error context */}
      <p className="text-xs text-red-300/80 leading-relaxed bg-red-950/30 rounded-xl p-2.5 border border-red-500/12 font-mono">
        {event.error_context || "No error context recorded"}
      </p>

      {/* Meta row */}
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-600">
        <span>Table <span className="text-gray-400">{event.table_name || "—"}</span></span>
        <span>Attempts <span className="text-red-400">{event.attempts}</span></span>
        {event.opco_id && (
          <span>Region <span className="text-gray-400">{event.opco_id}</span></span>
        )}
        <span>Failed <span className="text-gray-400">{fmtDate(event.failed_at)}</span></span>
      </div>

      {/* Expandable payload */}
      <button
        onClick={() => setExpanded((e) => !e)}
        className="text-xs text-gray-600 hover:text-gray-400 transition-colors text-left flex items-center gap-1"
      >
        <span>{expanded ? "▲" : "▼"}</span>
        <span>Payload details</span>
      </button>
      <AnimatePresence>
        {expanded && (
          <motion.pre
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.2 }}
            className="text-[11px] text-gray-400 bg-black/35 rounded-xl p-2.5 overflow-x-auto font-mono border border-white/5"
          >
            {JSON.stringify(
              { where_clause: event.where_clause, set_values: event.set_values },
              null,
              2,
            )}
          </motion.pre>
        )}
      </AnimatePresence>

      {/* Actions */}
      {!isAck && (
        <div className="flex gap-2 pt-1 border-t border-white/5">
          <button
            onClick={() => ackMutation.mutate()}
            disabled={ackMutation.isPending}
            className="flex-1 py-1.5 bg-white/5 hover:bg-white/8 text-gray-400 hover:text-gray-200 text-xs font-semibold rounded-xl transition-all border border-white/8 hover:border-white/15 disabled:opacity-40"
          >
            {ackMutation.isPending ? "Acknowledging…" : "✓ Acknowledge"}
          </button>
        </div>
      )}
    </motion.div>
  );
}

// ── Public component ─────────────────────────────────────────────────────────

interface Props {
  operatorId: string;
}

export function DLQPanel({ operatorId }: Props) {
  const [statusFilter, setStatusFilter] = useState<"OPEN" | "ACKNOWLEDGED" | "ALL">("OPEN");

  const { data: events, isLoading, error } = useQuery({
    queryKey: ["dlq", statusFilter],
    queryFn: () => fetchDlqEvents(statusFilter),
    refetchInterval: 20_000,
  });

  return (
    <div className="flex flex-col gap-4">
      {/* Sub-filter */}
      <div className="flex items-center gap-2">
        {(["OPEN", "ACKNOWLEDGED", "ALL"] as const).map((s) => (
          <button
            key={s}
            onClick={() => setStatusFilter(s)}
            className={clsx(
              "px-3 py-1 text-xs font-semibold rounded-lg transition-colors border",
              statusFilter === s
                ? "bg-red-500/12 text-red-300 border-red-500/30"
                : "bg-white/3 text-gray-600 border-white/6 hover:text-gray-300",
            )}
          >
            {s}
          </button>
        ))}
        <span className="ml-auto text-xs text-gray-700">
          {events?.length ?? 0} event{events?.length !== 1 ? "s" : ""}
        </span>
      </div>

      {isLoading && (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <div key={i} className="skeleton rounded-2xl h-28" />
          ))}
        </div>
      )}

      {error && (
        <p className="text-red-400/80 text-sm">
          {error instanceof Error ? error.message : "Failed to load DLQ events"}
        </p>
      )}

      {!isLoading && !error && (!events || events.length === 0) && (
        <div className="flex flex-col items-center justify-center h-36 text-gray-600 text-sm gap-3">
          <span className="text-4xl opacity-30 select-none">✓</span>
          <span>No DLQ events in <span className="text-gray-500 font-semibold">{statusFilter}</span> state</span>
        </div>
      )}

      <AnimatePresence>
        {events?.map((evt) => (
          <DlqRow key={evt.dlq_id || evt.outbox_id} event={evt} operatorId={operatorId} />
        ))}
      </AnimatePresence>
    </div>
  );
}
