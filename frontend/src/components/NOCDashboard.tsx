import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { AnimatePresence, motion } from "framer-motion";
import toast from "react-hot-toast";
import { clsx } from "clsx";
import { fetchActions, fetchQueueDepths, fetchSystemStatus, setStoredToken } from "../api/client";
import { useWebSocket } from "../hooks/useWebSocket";
import { ActionCard } from "./ActionCard";
import { DLQPanel } from "./DLQPanel";
import { Timeline } from "./Timeline";
import type { NocEvent, SentimentLabel } from "../types";

const OPERATOR_ID =
  typeof window !== "undefined"
    ? localStorage.getItem("synapse_operator_id") ?? "noc-operator"
    : "noc-operator";

const OPERATOR_ROLE =
  typeof window !== "undefined"
    ? localStorage.getItem("synapse_role") ?? "operator"
    : "operator";

const OPCO_OPTIONS = [
  "DEFAULT",
  "UK_REGION",
  "DE_REGION",
  "ES_REGION",
  "IT_REGION",
  "ALL",
];

type Tab = "PENDING" | "APPROVED" | "EXECUTED" | "DLQ" | "ALL";

const TAB_STATUS: Record<Tab, string | undefined> = {
  PENDING:  "PENDING",
  APPROVED: "APPROVED",
  EXECUTED: "EXECUTED",
  DLQ:      "FAILED",
  ALL:      undefined,
};

type SentimentMap = Map<string, { label: SentimentLabel; score: number }>;

// ── Aurora background with mouse parallax ─────────────────────────────────

function AuroraBackground() {
  const orb1 = useRef<HTMLDivElement>(null);
  const orb2 = useRef<HTMLDivElement>(null);
  const orb3 = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function onMove(e: MouseEvent) {
      const x = (e.clientX / window.innerWidth  - 0.5) * 30;
      const y = (e.clientY / window.innerHeight - 0.5) * 30;
      if (orb1.current) orb1.current.style.transform = `translate(${x * 0.18}px, ${y * 0.18}px)`;
      if (orb2.current) orb2.current.style.transform = `translate(${-x * 0.12}px, ${y * 0.10}px)`;
      if (orb3.current) orb3.current.style.transform = `translate(${x * 0.10}px, ${-y * 0.12}px)`;
    }
    window.addEventListener("mousemove", onMove, { passive: true });
    return () => window.removeEventListener("mousemove", onMove);
  }, []);

  return (
    <div className="aurora-bg">
      <div ref={orb1} className="aurora-orb aurora-orb-1" />
      <div ref={orb2} className="aurora-orb aurora-orb-2" />
      <div ref={orb3} className="aurora-orb aurora-orb-3" />
    </div>
  );
}

// ── Status bar ────────────────────────────────────────────────────────────

function StatusBar({
  connected,
  opcoId,
  depths,
  systemStatus,
  angryCount,
}: {
  connected: boolean;
  opcoId: string;
  depths: Record<string, number> | undefined;
  systemStatus: Record<string, unknown> | undefined;
  angryCount: number;
}) {
  const stale =
    (systemStatus?.circuit_breaker as Record<string, unknown> | undefined)
      ?.status === "SYSTEM_STALE";

  return (
    <div className="flex items-center gap-4 px-5 py-1.5 glass-subtle border-b border-white/4 text-xs text-gray-500 flex-wrap shrink-0">
      <div className="flex items-center gap-1.5">
        <span
          className={clsx(
            "w-1.5 h-1.5 rounded-full",
            connected
              ? "bg-emerald-400 shadow-sm shadow-emerald-400/60 animate-pulse"
              : "bg-red-500",
          )}
        />
        <span className={connected ? "text-emerald-400/70" : "text-red-400/70"}>
          {connected ? `Live — ${opcoId}` : "Disconnected"}
        </span>
      </div>

      {depths && (
        <>
          <span>
            Pending: <strong className="text-amber-400/80">{depths.PENDING ?? 0}</strong>
          </span>
          <span>
            Approved: <strong className="text-emerald-400/80">{depths.APPROVED ?? 0}</strong>
          </span>
          <span>
            DLQ: <strong className="text-red-400/80">{depths.FAILED ?? 0}</strong>
          </span>
        </>
      )}

      <div className="ml-auto flex items-center gap-2">
        {angryCount > 0 && (
          <span className="flex items-center gap-1.5 px-2 py-0.5 bg-red-500/15 text-red-400 rounded-full font-bold border border-red-500/30 animate-pulse text-[10px] tracking-wider">
            <span className="w-1 h-1 rounded-full bg-red-400 inline-block" />
            {angryCount} ANGRY CUSTOMER{angryCount > 1 ? "S" : ""}
          </span>
        )}
        {stale && (
          <span className="px-2 py-0.5 bg-red-500/12 text-red-400 rounded-full font-bold border border-red-500/25 animate-pulse text-[10px] tracking-wider">
            SYSTEM_STALE
          </span>
        )}
      </div>
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────

export function NOCDashboard({ onLogout }: { onLogout?: () => void }) {
  // OPERATOR role: auto-filter to assigned region; MANAGER can see ALL
  const assignedOpco =
    typeof window !== "undefined" ? localStorage.getItem("synapse_opco_id") : null;
  const defaultOpco = OPERATOR_ROLE === "operator" && assignedOpco ? assignedOpco : "ALL";
  const [opcoId, setOpcoId] = useState(defaultOpco);
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

  const angryCount = useMemo(
    () => [...sentimentMap.values()].filter((s) => s.label === "ANGRY").length,
    [sentimentMap],
  );

  const sortedActions = useMemo(() => {
    if (!actions) return [];
    return [...actions].sort((a, b) => {
      const aAngry = sentimentMap.get(a.correlation_id)?.label === "ANGRY" ? -1 : 0;
      const bAngry = sentimentMap.get(b.correlation_id)?.label === "ANGRY" ? -1 : 0;
      return aAngry - bAngry;
    });
  }, [actions, sentimentMap]);

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-[#0f0a1e]">
      <AuroraBackground />

      <div className="relative z-10 flex flex-col h-full">
        {/* Header */}
        <motion.header
          initial={{ opacity: 0, y: -12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, ease: "easeOut" }}
          className="glass-header border-b border-white/5 px-5 py-3 flex items-center justify-between gap-4 shrink-0"
        >
          <div className="flex items-center gap-3">
            {/* Logo */}
            <motion.div
              initial={{ scale: 0.5, rotate: -20 }}
              animate={{ scale: 1, rotate: 0 }}
              transition={{ type: "spring", stiffness: 260, damping: 20, delay: 0.15 }}
              className="w-8 h-8 rounded-xl bg-gradient-to-br from-violet-500 to-purple-700 flex items-center justify-center text-sm font-black shadow-lg shadow-violet-500/40 shrink-0"
            >
              S
            </motion.div>
            <div>
              <h1 className="text-sm font-bold tracking-tight gradient-text leading-none">
                Synapse NOC
              </h1>
              <p className="text-[10px] text-gray-600 mt-0.5 hidden sm:block">
                Telco Fallout Dashboard
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <select
              value={opcoId}
              onChange={(e) => setOpcoId(e.target.value)}
              disabled={OPERATOR_ROLE === "operator"}
              title={OPERATOR_ROLE === "operator" ? "Region locked to your assigned OpCo" : undefined}
              className={clsx(
                "bg-white/4 border border-white/8 rounded-lg px-2.5 py-1 text-xs text-gray-300 transition-colors",
                OPERATOR_ROLE === "operator"
                  ? "opacity-60 cursor-not-allowed"
                  : "focus:outline-none focus:border-blue-500/50",
              )}
            >
              {OPCO_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
            <div className="flex items-center gap-2">
              <span className="text-xs text-gray-600 hidden sm:inline font-mono">
                {OPERATOR_ID}
              </span>
              <span className={clsx(
                "text-[10px] px-1.5 py-0.5 rounded-full font-bold uppercase tracking-wider",
                OPERATOR_ROLE === "manager"
                  ? "bg-violet-500/15 text-violet-300 border border-violet-500/25"
                  : "bg-white/5 text-gray-500 border border-white/8",
              )}>
                {OPERATOR_ROLE}
              </span>
              {onLogout && (
                <button
                  onClick={() => {
                    setStoredToken(null);
                    localStorage.removeItem("synapse_operator_id");
                    localStorage.removeItem("synapse_role");
                    localStorage.removeItem("synapse_opco_id");
                    onLogout();
                  }}
                  className="text-xs text-gray-600 hover:text-red-400 transition-colors px-2 py-1 rounded-lg hover:bg-red-500/8 border border-transparent hover:border-red-500/15"
                >
                  Sign out
                </button>
              )}
            </div>
          </div>
        </motion.header>

        {/* Status bar */}
        <StatusBar
          connected={connected}
          opcoId={opcoId}
          depths={depths as Record<string, number>}
          systemStatus={systemStatus}
          angryCount={angryCount}
        />

        {/* Tab strip */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.4, delay: 0.2 }}
          className="flex border-b border-white/5 glass-subtle shrink-0 px-4"
        >
          {(["PENDING", "APPROVED", "EXECUTED", "DLQ", "ALL"] as Tab[]).map((t) => {
            const depthMap = depths as Record<string, number> | undefined;
            const badgeCount =
              t === "PENDING" ? depthMap?.PENDING :
              t === "DLQ"     ? depthMap?.FAILED  : undefined;
            const isDlq = t === "DLQ";
            return (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={clsx(
                  "relative px-4 py-2.5 text-xs font-semibold transition-colors duration-200",
                  tab === t
                    ? isDlq ? "text-red-400" : "text-violet-400"
                    : "text-gray-600 hover:text-gray-300",
                )}
              >
                {tab === t && (
                  <motion.div
                    layoutId="active-tab"
                    className={clsx(
                      "absolute bottom-0 left-0 right-0 h-0.5 rounded-full",
                      isDlq
                        ? "bg-gradient-to-r from-red-500 to-orange-400"
                        : "bg-gradient-to-r from-violet-500 to-purple-400",
                    )}
                    transition={{ type: "spring", stiffness: 420, damping: 36 }}
                  />
                )}
                {isDlq ? "DLQ / REPAIR" : t}
                {badgeCount ? (
                  <motion.span
                    key={badgeCount}
                    initial={{ scale: 0.6 }}
                    animate={{ scale: 1 }}
                    className={clsx(
                      "ml-1.5 px-1.5 py-0.5 rounded-full text-[10px] border",
                      isDlq
                        ? "bg-red-500/15 text-red-300/90 border-red-500/25"
                        : "bg-amber-500/15 text-amber-300/90 border-amber-500/25",
                    )}
                  >
                    {badgeCount}
                  </motion.span>
                ) : null}
              </button>
            );
          })}
        </motion.div>

        {/* Main content */}
        <main className="flex-1 overflow-y-auto p-5">
          {tab === "DLQ" ? (
            <DLQPanel operatorId={OPERATOR_ID} />
          ) : (
            <AnimatePresence mode="wait">
              {sortedActions.length === 0 ? (
                <motion.div
                  key="empty"
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.3 }}
                  className="flex flex-col items-center justify-center h-48 text-gray-600 text-sm gap-3"
                >
                  <span className="text-4xl opacity-30 select-none">◎</span>
                  <span>
                    No actions in{" "}
                    <span className="text-gray-500 font-semibold">{tab}</span> state
                  </span>
                </motion.div>
              ) : (
                <motion.div
                  key={tab}
                  className="grid grid-cols-1 xl:grid-cols-2 gap-4"
                >
                  <AnimatePresence>
                    {sortedActions.map((action, i) => {
                      const sentiment = sentimentMap.get(action.correlation_id);
                      return (
                        <motion.div
                          key={action.id}
                          initial={{ opacity: 0, y: 22 }}
                          animate={{ opacity: 1, y: 0 }}
                          exit={{ opacity: 0, scale: 0.96 }}
                          transition={{
                            duration: 0.38,
                            delay: i * 0.055,
                            ease: [0.22, 1, 0.36, 1],
                          }}
                          layout
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
                </motion.div>
              )}
            </AnimatePresence>
          )}
        </main>

        {/* Timeline drawer */}
        <AnimatePresence>
          {traceId && (
            <Timeline traceId={traceId} onClose={() => setTraceId(null)} />
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
