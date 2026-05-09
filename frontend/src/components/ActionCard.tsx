import { useRef, useEffect, useState } from "react";
import { motion } from "framer-motion";
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

const STATUS_COLOURS: Record<string, string> = {
  PENDING:         "border-gray-700",
  APPROVED:        "border-green-700",
  REJECTED:        "border-red-800",
  EXPIRED:         "border-gray-700 opacity-60",
  STALE:           "border-amber-700",
  ESCALATED_STALE: "border-orange-600",
  EXECUTING:       "border-blue-700",
  EXECUTED:        "border-green-800 opacity-70",
  FAILED:          "border-red-700",
};

const ACTION_TYPE_ICONS: Record<string, string> = {
  retry_provisioning: "⚙️",
  update_billing:     "💳",
  cancel_order:       "🚫",
  create_ticket:      "🎫",
  escalate:           "🚨",
  notify_customer:    "📨",
  update_subscription:"📋",
  manual_review:      "👁",
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

  // Shake + pulse-border when angry customer sentiment is detected
  const isAngry = sentimentLabel === "ANGRY";

  useEffect(() => {
    if (triggerShake || isAngry) {
      setShaking(true);
      const t = setTimeout(() => setShaking(false), 700);
      return () => clearTimeout(t);
    }
  }, [triggerShake, isAngry]);

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
    ? "border-red-500 animate-pulse-border"
    : STATUS_COLOURS[action.status] ?? "border-gray-700";

  const isPending = action.status === "PENDING" || action.status === "ESCALATED_STALE";
  const icon = ACTION_TYPE_ICONS[action.action_type] ?? "🔧";

  return (
    <motion.div
      ref={cardRef}
      layout
      className={clsx(
        "rounded-xl bg-gray-900 border-2 p-4 flex flex-col gap-3 transition-all",
        borderClass,
        shaking && "animate-shake",
        isAngry && "shadow-lg shadow-red-900/30",
      )}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-xl">{icon}</span>
          <div className="min-w-0">
            <p className="font-semibold text-sm text-gray-100 truncate">
              {action.action_type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
            </p>
            <p className="text-xs text-gray-500 font-mono truncate">{action.id}</p>
          </div>
        </div>

        <div className="flex flex-col items-end gap-1 shrink-0">
          <span
            className={clsx(
              "text-[11px] font-bold px-2 py-0.5 rounded-full uppercase",
              {
                "bg-amber-900 text-amber-300": action.status === "PENDING",
                "bg-green-900 text-green-300": action.status === "APPROVED" || action.status === "EXECUTED",
                "bg-red-900 text-red-300":     action.status === "REJECTED" || action.status === "FAILED",
                "bg-orange-900 text-orange-300": action.status === "ESCALATED_STALE",
                "bg-blue-900 text-blue-300":   action.status === "EXECUTING",
                "bg-gray-800 text-gray-400":   action.status === "EXPIRED" || action.status === "STALE",
              },
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
      <p className="text-sm text-gray-300 leading-relaxed">{action.action_description}</p>

      {/* Meta row */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500">
        {action.customer_id && <span>Customer #{action.customer_id}</span>}
        {action.order_id && <span>Order #{action.order_id}</span>}
        <span>Created {fmtDate(action.created_at)}</span>
        <span>Expires {fmtDate(action.expires_at)}</span>
      </div>

      {/* State diff */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="bg-gray-950 rounded p-2">
          <p className="text-gray-500 mb-1 font-semibold uppercase tracking-wide text-[10px]">Before</p>
          <pre className="text-gray-400 whitespace-pre-wrap break-all leading-relaxed">
            {JSON.stringify(action.predicted_before_state, null, 2)}
          </pre>
        </div>
        <div className="bg-gray-950 rounded p-2">
          <p className="text-gray-500 mb-1 font-semibold uppercase tracking-wide text-[10px]">After</p>
          <pre className="text-green-400 whitespace-pre-wrap break-all leading-relaxed">
            {JSON.stringify(action.predicted_after_state, null, 2)}
          </pre>
        </div>
      </div>

      {/* AI thought + feedback */}
      <AIThoughtPanel action={action} />
      <HumanFeedback
        actionId={action.id}
        actionType={action.action_type}
        operatorId={operatorId}
      />

      {/* Trace link */}
      <button
        onClick={() => onViewTrace(action.correlation_id)}
        className="text-xs text-gray-500 hover:text-blue-400 transition-colors text-left"
      >
        View trace ↗ <span className="font-mono">{action.correlation_id.slice(0, 12)}…</span>
      </button>

      {/* HITL actions */}
      {isPending && (
        <div className="flex flex-col gap-2 pt-1 border-t border-gray-800">
          {showReject ? (
            <div className="space-y-2">
              <input
                type="text"
                placeholder="Rejection reason (required)"
                value={rejectReason}
                onChange={(e) => setRejectReason(e.target.value)}
                className="w-full bg-gray-950 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:border-red-500"
              />
              <div className="flex gap-2">
                <button
                  onClick={() => rejectMutation.mutate()}
                  disabled={!rejectReason.trim() || rejectMutation.isPending}
                  className="flex-1 py-2 bg-red-700 hover:bg-red-600 text-white text-sm font-semibold rounded transition-colors disabled:opacity-40"
                >
                  {rejectMutation.isPending ? "Rejecting…" : "Confirm Reject"}
                </button>
                <button
                  onClick={() => setShowReject(false)}
                  className="px-4 py-2 text-sm text-gray-400 hover:text-gray-200"
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
                className="flex-1 py-2 bg-green-700 hover:bg-green-600 text-white text-sm font-semibold rounded transition-colors disabled:opacity-40"
              >
                {approveMutation.isPending ? "Approving…" : "✓ Approve"}
              </button>
              <button
                onClick={() => setShowReject(true)}
                className="flex-1 py-2 bg-gray-800 hover:bg-red-900 text-gray-300 hover:text-red-300 text-sm font-semibold rounded transition-colors border border-gray-700 hover:border-red-700"
              >
                ✗ Reject
              </button>
            </div>
          )}
        </div>
      )}

      {action.rejection_reason && (
        <p className="text-xs text-red-400">
          Rejected: {action.rejection_reason}
        </p>
      )}
    </motion.div>
  );
}
