import { useRef, useEffect, useState } from "react";
import { motion, useMotionValue, useTransform, useSpring } from "framer-motion";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import toast from "react-hot-toast";
import { clsx } from "clsx";
import { approveAction, rejectAction } from "../api/client";
import { AIThoughtPanel } from "./AIThoughtPanel";
import { HumanFeedback } from "./HumanFeedback";
import { SentimentBadge } from "./SentimentBadge";
import type { ActionProposal, SentimentLabel } from "../types";

interface Props {
  action: ActionProposal;
  operatorId: string;
  sentimentLabel?: SentimentLabel | null;
  sentimentScore?: number | null;
  onViewTrace: (correlationId: string) => void;
  triggerShake?: boolean;
}

const STATUS_BORDER: Record<string, string> = {
  PENDING:         "border-amber-500/25",
  APPROVED:        "border-emerald-500/30",
  REJECTED:        "border-red-500/25",
  EXPIRED:         "border-white/6 opacity-60",
  STALE:           "border-amber-600/25",
  ESCALATED_STALE: "border-orange-500/40",
  EXECUTING:       "border-blue-500/35",
  EXECUTED:        "border-emerald-600/25 opacity-70",
  FAILED:          "border-red-600/35",
};

const STATUS_BADGE: Record<string, string> = {
  PENDING:         "bg-amber-500/12 text-amber-300 border border-amber-500/25",
  APPROVED:        "bg-emerald-500/12 text-emerald-300 border border-emerald-500/25",
  REJECTED:        "bg-red-500/12 text-red-300 border border-red-500/25",
  EXPIRED:         "bg-white/5 text-gray-500 border border-white/8",
  STALE:           "bg-amber-500/10 text-amber-400 border border-amber-500/20",
  ESCALATED_STALE: "bg-orange-500/12 text-orange-300 border border-orange-500/25",
  EXECUTING:       "bg-blue-500/12 text-blue-300 border border-blue-500/25",
  EXECUTED:        "bg-emerald-600/10 text-emerald-400 border border-emerald-600/20",
  FAILED:          "bg-red-600/12 text-red-300 border border-red-600/25",
};

const ACTION_TYPE_ICONS: Record<string, string> = {
  retry_provisioning:  "⚙️",
  update_billing:      "💳",
  cancel_order:        "🚫",
  create_ticket:       "🎫",
  escalate:            "🚨",
  notify_customer:     "📨",
  update_subscription: "📋",
  manual_review:       "👁",
};

function fmtDate(iso: string) {
  return new Date(iso).toLocaleString([], {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

export function ActionCard({
  action,
  operatorId,
  sentimentLabel,
  sentimentScore,
  onViewTrace,
  triggerShake = false,
}: Props) {
  const qc = useQueryClient();
  const cardRef = useRef<HTMLDivElement>(null);
  const [shaking, setShaking] = useState(false);
  const [rejectReason, setRejectReason] = useState("");
  const [showReject, setShowReject] = useState(false);
  const [glowPos, setGlowPos] = useState({ x: 50, y: 50 });
  const [isHovering, setIsHovering] = useState(false);

  const isAngry = sentimentLabel === "ANGRY";

  // Subtle 3-D tilt — reduced to ±2° for a calmer enterprise feel
  const rawX = useMotionValue(0);
  const rawY = useMotionValue(0);
  const rotateX = useSpring(useTransform(rawY, [-1, 1], [2, -2]), { stiffness: 200, damping: 60 });
  const rotateY = useSpring(useTransform(rawX, [-1, 1], [-2, 2]), { stiffness: 200, damping: 60 });

  useEffect(() => {
    if (triggerShake || isAngry) {
      setShaking(true);
      const t = setTimeout(() => setShaking(false), 700);
      return () => clearTimeout(t);
    }
  }, [triggerShake, isAngry]);

  function handleMouseMove(e: React.MouseEvent<HTMLDivElement>) {
    if (!cardRef.current) return;
    const rect = cardRef.current.getBoundingClientRect();
    rawX.set(((e.clientX - rect.left) / rect.width)  * 2 - 1);
    rawY.set(((e.clientY - rect.top)  / rect.height) * 2 - 1);
    setGlowPos({
      x: ((e.clientX - rect.left) / rect.width)  * 100,
      y: ((e.clientY - rect.top)  / rect.height) * 100,
    });
  }

  function handleMouseEnter() { setIsHovering(true); }
  function handleMouseLeave() {
    rawX.set(0);
    rawY.set(0);
    setIsHovering(false);
  }

  const approveMutation = useMutation({
    mutationFn: () => approveAction(action.id, operatorId),
    onSuccess: () => {
      toast.success(`Approved: ${action.action_type}`);
      qc.invalidateQueries({ queryKey: ["actions"] });
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const rejectMutation = useMutation({
    mutationFn: () => rejectAction(action.id, operatorId, rejectReason),
    onSuccess: () => {
      toast(`Rejected: ${action.action_type}`, { icon: "✗" });
      setShowReject(false);
      qc.invalidateQueries({ queryKey: ["actions"] });
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const borderClass = isAngry
    ? "border-red-500/55 animate-pulse-border"
    : STATUS_BORDER[action.status] ?? "border-white/6";

  const isPending = action.status === "PENDING" || action.status === "ESCALATED_STALE";
  const icon = ACTION_TYPE_ICONS[action.action_type] ?? "🔧";
  const badgeClass = STATUS_BADGE[action.status] ?? "bg-white/5 text-gray-400 border border-white/8";

  return (
    <motion.div
      ref={cardRef}
      layout
      style={{ rotateX, rotateY, transformPerspective: 1200 }}
      onMouseMove={handleMouseMove}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      className={clsx(
        "rounded-2xl glass border p-4 flex flex-col gap-3 relative overflow-hidden card-hover-glow",
        isAngry
          ? "shadow-xl shadow-red-900/30 hover:shadow-2xl hover:shadow-red-900/40"
          : "hover:shadow-2xl hover:shadow-violet-950/60",
        borderClass,
        shaking && "animate-shake",
      )}
    >
      {/* Mouse-following gradient glow */}
      <div
        className={clsx(
          "absolute inset-0 rounded-2xl pointer-events-none transition-opacity duration-500",
          isHovering ? "opacity-100" : "opacity-0",
        )}
        style={{
          background: `radial-gradient(circle at ${glowPos.x}% ${glowPos.y}%, rgba(139,92,246,0.12) 0%, transparent 62%)`,
        }}
      />

      {/* Header */}
      <div className="flex items-start justify-between gap-2 relative">
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-9 h-9 rounded-xl bg-white/5 border border-white/6 flex items-center justify-center text-lg shrink-0">
            {icon}
          </div>
          <div className="min-w-0">
            <p className="font-semibold text-sm text-gray-100 truncate">
              {action.action_type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
            </p>
            <p className="text-[11px] text-gray-600 font-mono truncate">{action.id}</p>
          </div>
        </div>

        <div className="flex flex-col items-end gap-1.5 shrink-0">
          <span
            className={clsx(
              "text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wider",
              badgeClass,
            )}
          >
            {action.status}
          </span>
          {sentimentLabel && (
            <SentimentBadge label={sentimentLabel} score={sentimentScore} />
          )}
        </div>
      </div>

      {/* Description */}
      <p className="text-sm text-gray-300/90 leading-relaxed relative">
        {action.action_description}
      </p>

      {/* Meta row */}
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-600 relative">
        {action.customer_id && (
          <span>Customer <span className="text-gray-400">#{action.customer_id}</span></span>
        )}
        {action.order_id && (
          <span>Order <span className="text-gray-400">#{action.order_id}</span></span>
        )}
        <span>Created <span className="text-gray-400">{fmtDate(action.created_at)}</span></span>
        <span>Expires <span className="text-gray-400">{fmtDate(action.expires_at)}</span></span>
      </div>

      {/* State diff */}
      <div className="grid grid-cols-2 gap-2 text-xs relative">
        <div className="bg-black/35 rounded-xl p-2.5 border border-white/4">
          <p className="text-gray-600 mb-1.5 font-semibold uppercase tracking-widest text-[9px]">
            Before
          </p>
          <pre className="text-gray-400 whitespace-pre-wrap break-all leading-relaxed">
            {JSON.stringify(action.predicted_before_state, null, 2)}
          </pre>
        </div>
        <div className="bg-black/35 rounded-xl p-2.5 border border-white/4">
          <p className="text-gray-600 mb-1.5 font-semibold uppercase tracking-widest text-[9px]">
            After
          </p>
          <pre className="text-emerald-400 whitespace-pre-wrap break-all leading-relaxed">
            {JSON.stringify(action.predicted_after_state, null, 2)}
          </pre>
        </div>
      </div>

      {/* AI thought + feedback */}
      <div className="relative">
        <AIThoughtPanel action={action} />
        <HumanFeedback
          actionId={action.id}
          actionType={action.action_type}
          operatorId={operatorId}
        />
      </div>

      {/* Trace link */}
      <button
        onClick={() => onViewTrace(action.correlation_id)}
        className="group text-xs text-gray-600 hover:text-blue-400 transition-colors text-left relative flex items-center gap-1"
      >
        <span className="group-hover:text-blue-300 transition-colors">View trace</span>
        <span className="opacity-60 group-hover:opacity-100 transition-opacity">↗</span>
        <span className="font-mono text-gray-600 group-hover:text-blue-400/70 transition-colors">
          {action.correlation_id.slice(0, 12)}…
        </span>
      </button>

      {/* HITL actions */}
      {isPending && (
        <div className="flex flex-col gap-2 pt-2 border-t border-white/5 relative">
          {showReject ? (
            <div className="space-y-2">
              <input
                type="text"
                placeholder="Rejection reason (required)"
                value={rejectReason}
                onChange={(e) => setRejectReason(e.target.value)}
                className="w-full bg-black/40 border border-white/8 rounded-xl px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:border-red-500/50 placeholder-gray-700 transition-colors"
              />
              <div className="flex gap-2">
                <button
                  onClick={() => rejectMutation.mutate()}
                  disabled={!rejectReason.trim() || rejectMutation.isPending}
                  className="flex-1 py-2 bg-red-600/80 hover:bg-red-500/90 text-white text-sm font-semibold rounded-xl transition-all duration-200 disabled:opacity-40 shadow-lg shadow-red-900/20"
                >
                  {rejectMutation.isPending ? "Rejecting…" : "Confirm Reject"}
                </button>
                <button
                  onClick={() => setShowReject(false)}
                  className="px-4 py-2 text-sm text-gray-500 hover:text-gray-300 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <div className="flex gap-2">
              <button
                onClick={() => approveMutation.mutate()}
                disabled={approveMutation.isPending}
                className="flex-1 py-2 bg-gradient-to-r from-emerald-600 to-green-600 hover:from-emerald-500 hover:to-green-500 text-white text-sm font-semibold rounded-xl transition-all duration-200 disabled:opacity-40 shadow-md shadow-emerald-900/25"
              >
                {approveMutation.isPending ? "Approving…" : "✓ Approve"}
              </button>
              <button
                onClick={() => setShowReject(true)}
                className="flex-1 py-2 bg-white/4 hover:bg-red-900/25 text-gray-400 hover:text-red-300 text-sm font-semibold rounded-xl transition-all duration-200 border border-white/6 hover:border-red-600/35"
              >
                ✗ Reject
              </button>
            </div>
          )}
        </div>
      )}

      {action.rejection_reason && (
        <p className="text-xs text-red-400/75 relative">
          Rejected: {action.rejection_reason}
        </p>
      )}
    </motion.div>
  );
}
