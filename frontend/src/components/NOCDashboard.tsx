import { useState, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { AnimatePresence, motion } from "framer-motion";
import toast from "react-hot-toast";
import { clsx } from "clsx";
import { fetchActions, fetchQueueDepths, fetchSystemStatus } from "../api/client";
import { useWebSocket } from "../hooks/useWebSocket";
import { ActionCard } from "./ActionCard";
import { Timeline } from "./Timeline";
import type { NocEvent, SentimentLabel } from "../types";

const OPERATOR_ID =
  typeof window !== "undefined"
    ? localStorage.getItem("synapse_operator_id") ?? "noc-operator"
    : "noc-operator";

const OPCO_OPTIONS = [
  "DEFAULT",
  "VODAFONE_UK",
  "VODAFONE_DE",
  "VODAFONE_ES",
  "VODAFONE_IT",
  "ALL",
];

type Tab = "PENDING" | "APPROVED" | "EXECUTED" | "ALL";

const TAB_STATUS: Record<Tab, string | undefined> = {
  PENDING:  "PENDING",
  APPROVED: "APPROVED",
  EXECUTED: "EXECUTED",
  ALL:      undefined,
};

// sentiment state keyed by correlation_id
type SentimentMap = Map<string, { label: SentimentLabel; score: number }>;

function StatusBar({
  connected,
  opcoId,
  depths,
  systemStatus,
}: {
  connected: boolean;
  opcoId: string;
  depths: Record<string, number> | undefined;
  systemStatus: Record<string, unknown> | undefined;
}) {
  const stale =
    (systemStatus?.circuit_breaker as Record<string, unknown> | undefined)
      ?.status === "SYSTEM_STALE";

  return (
    <div className="flex items-center gap-4 px-5 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400 flex-wrap">
      <div className="flex items-center gap-1.5">
        <span
          className={clsx(
            "w-2 h-2 rounded-full",
            connected ? "bg-green-500 animate-pulse" : "bg-red-500",
          )}
        />
        <span>{connected ? `WS live — ${opcoId}` : "WS disconnected"}</span>
      </div>

      {depths && (
        <>
          <span>Pending: <strong className="text-amber-400">{depths.PENDING ?? 0}</strong></span>
          <span>Approved: <strong className="text-green-400">{depths.APPROVED ?? 0}</strong></span>
          <span>DLQ: <strong className="text-red-400">{depths.FAILED ?? 0}</strong></span>
        </>
      )}

      {stale && (
        <span className="ml-auto px-2 py-0.5 bg-red-900 text-red-300 rounded-full font-bold animate-pulse">
          SYSTEM_STALE
        </span>
      )}
    </div>
  );
}

export function NOCDashboard() {
  const [opcoId, setOpcoId] = useState("DEFAULT");
  const [tab, setTab] = useState<Tab>("PENDING");
  const [traceId, setTraceId] = useState<string | null>(null);
  const [sentimentMap, setSentimentMap] = useState<SentimentMap>(new Map());

  const { data: actions, refetch: refetchActions } = useQuery({
    queryKey: ["actions", TAB_STATUS[tab]],
    queryFn: () => fetchActions(TAB_STATUS[tab]),
    refetchInterval: 15_000,
  });

  const { data: depths } = useQuery({
    queryKey: ["queue-depths"],
    queryFn: fetchQueueDepths,
    refetchInterval: 10_000,
  });

  const { data: systemStatus } = useQuery({
    queryKey: ["system-status"],
    queryFn: fetchSystemStatus,
    refetchInterval: 30_000,
  });

  const handleWsEvent = useCallback(
    (evt: NocEvent) => {
      if (evt.event_type === "heartbeat") return;

      if (evt.event_type === "sentiment_alert" && evt.sentiment_label === "ANGRY") {
        const cid = String(evt.correlation_id ?? "");
        setSentimentMap((prev) => {
          const next = new Map(prev);
          next.set(cid, {
            label: evt.sentiment_label as SentimentLabel,
            score: (evt.sentiment_score as number) ?? -1,
          });
          return next;
        });
        toast.error(
          `⚠️ ANGRY customer detected — correlation ${String(cid).slice(0, 8)}…`,
          { duration: 8000 },
        );
        refetchActions();
        return;
      }

      if (
        evt.event_type === "resolution_proposed" ||
        evt.event_type === "action_approved" ||
        evt.event_type === "force_approved"
      ) {
        const label =
          evt.event_type === "resolution_proposed"
            ? "📋 New resolution proposed"
            : evt.event_type === "force_approved"
            ? "⚡ Force-approved by operator"
            : "✅ Action approved";
        toast(label, { icon: undefined });
        refetchActions();
      }
    },
    [refetchActions],
  );

  const { connected } = useWebSocket({ opcoId, onEvent: handleWsEvent });

  return (
    <div className="flex flex-col h-screen overflow-hidden">
      {/* Top bar */}
      <header className="px-5 py-3 bg-gray-900 border-b border-gray-800 flex items-center justify-between gap-4 shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center text-sm font-black">
            S
          </div>
          <h1 className="text-base font-bold text-gray-100 tracking-tight">
            Synapse NOC
          </h1>
          <span className="text-xs text-gray-600 hidden sm:inline">
            Telco Fallout Dashboard
          </span>
        </div>

        <div className="flex items-center gap-3">
          <select
            value={opcoId}
            onChange={(e) => setOpcoId(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-300 focus:outline-none focus:border-blue-500"
          >
            {OPCO_OPTIONS.map((o) => (
              <option key={o} value={o}>{o}</option>
            ))}
          </select>
          <span className="text-xs text-gray-500 hidden sm:inline">
            {OPERATOR_ID}
          </span>
        </div>
      </header>

      {/* Status bar */}
      <StatusBar
        connected={connected}
        opcoId={opcoId}
        depths={depths as Record<string, number>}
        systemStatus={systemStatus}
      />

      {/* Tab strip */}
      <div className="flex border-b border-gray-800 bg-gray-950 shrink-0 px-4">
        {(["PENDING", "APPROVED", "EXECUTED", "ALL"] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={clsx(
              "px-4 py-2.5 text-xs font-semibold border-b-2 transition-colors",
              tab === t
                ? "border-blue-500 text-blue-400"
                : "border-transparent text-gray-500 hover:text-gray-300",
            )}
          >
            {t}
            {t === "PENDING" && depths?.PENDING ? (
              <span className="ml-1.5 px-1.5 py-0.5 bg-amber-800 text-amber-200 rounded-full text-[10px]">
                {depths.PENDING}
              </span>
            ) : null}
          </button>
        ))}
      </div>

      {/* Action grid */}
      <main className="flex-1 overflow-y-auto p-5">
        {!actions || actions.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-48 text-gray-600 text-sm gap-2">
            <span className="text-3xl">🔍</span>
            <span>No actions in {tab} state</span>
          </div>
        ) : (
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            <AnimatePresence>
              {actions.map((action) => {
                const sentiment = sentimentMap.get(action.correlation_id);
                return (
                  <motion.div
                    key={action.id}
                    initial={{ opacity: 0, y: 12 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                  >
                    <ActionCard
                      action={action}
                      operatorId={OPERATOR_ID}
                      sentimentLabel={sentiment?.label}
                      sentimentScore={sentiment?.score}
                      onViewTrace={(cid) => setTraceId(cid)}
                      triggerShake={!!sentiment && sentiment.label === "ANGRY"}
                    />
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </div>
        )}
      </main>

      {/* Timeline drawer */}
      <AnimatePresence>
        {traceId && (
          <Timeline traceId={traceId} onClose={() => setTraceId(null)} />
        )}
      </AnimatePresence>
    </div>
  );
}
