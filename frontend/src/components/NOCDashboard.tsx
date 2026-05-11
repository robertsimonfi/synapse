import { useState, useCallback, useRef, useEffect } from "react";
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

type SentimentMap = Map<string, { label: SentimentLabel; score: number }>;

// ── Aurora background with mouse parallax ─────────────────────────────────

function AuroraBackground() {
  const orb1 = useRef<HTMLDivElement>(null);
  const orb2 = useRef<HTMLDivElement>(null);
  const orb3 = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function onMove(e: MouseEvent) {
      const x = (e.clientX / window.innerWidth  - 0.5) * 60;
      const y = (e.clientY / window.innerHeight - 0.5) * 60;
      if (orb1.current) orb1.current.style.transform = `translate(${x * 0.35}px, ${y * 0.35}px)`;
      if (orb2.current) orb2.current.style.transform = `translate(${-x * 0.25}px, ${y * 0.20}px)`;
      if (orb3.current) orb3.current.style.transform = `translate(${x * 0.20}px, ${-y * 0.25}px)`;
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

      {stale && (
        <span className="ml-auto px-2 py-0.5 bg-red-500/12 text-red-400 rounded-full font-bold border border-red-500/25 animate-pulse text-[10px] tracking-wider">
          SYSTEM_STALE
        </span>
      )}
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────

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
    <div className="flex flex-col h-screen overflow-hidden bg-[#030712]">
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
              className="w-8 h-8 rounded-xl bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-sm font-black shadow-lg shadow-blue-500/30 shrink-0"
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
              className="bg-white/4 border border-white/8 rounded-lg px-2.5 py-1 text-xs text-gray-300 focus:outline-none focus:border-blue-500/50 transition-colors"
            >
              {OPCO_OPTIONS.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
            <span className="text-xs text-gray-600 hidden sm:inline font-mono">
              {OPERATOR_ID}
            </span>
          </div>
        </motion.header>

        {/* Status bar */}
        <StatusBar
          connected={connected}
          opcoId={opcoId}
          depths={depths as Record<string, number>}
          systemStatus={systemStatus}
        />

        {/* Tab strip */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.4, delay: 0.2 }}
          className="flex border-b border-white/5 glass-subtle shrink-0 px-4"
        >
          {(["PENDING", "APPROVED", "EXECUTED", "ALL"] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={clsx(
                "relative px-4 py-2.5 text-xs font-semibold transition-colors duration-200",
                tab === t ? "text-blue-400" : "text-gray-600 hover:text-gray-300",
              )}
            >
              {tab === t && (
                <motion.div
                  layoutId="active-tab"
                  className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-blue-500 to-violet-500 rounded-full"
                  transition={{ type: "spring", stiffness: 420, damping: 36 }}
                />
              )}
              {t}
              {t === "PENDING" && (depths as Record<string, number> | undefined)?.PENDING ? (
                <motion.span
                  key={(depths as Record<string, number>).PENDING}
                  initial={{ scale: 0.6 }}
                  animate={{ scale: 1 }}
                  className="ml-1.5 px-1.5 py-0.5 bg-amber-500/15 text-amber-300/90 rounded-full text-[10px] border border-amber-500/25"
                >
                  {(depths as Record<string, number>).PENDING}
                </motion.span>
              ) : null}
            </button>
          ))}
        </motion.div>

        {/* Action grid */}
        <main className="flex-1 overflow-y-auto p-5">
          <AnimatePresence mode="wait">
            {!actions || actions.length === 0 ? (
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
                  {actions.map((action, i) => {
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
