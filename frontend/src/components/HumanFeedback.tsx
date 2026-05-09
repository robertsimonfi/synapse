import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import toast from "react-hot-toast";
import { submitFeedback } from "../api/client";
import type { HumanFeedbackPayload } from "../types";

interface Props {
  actionId: string;
  actionType: string;
  operatorId: string;
}

const ACTION_OPTIONS = [
  "retry_provisioning",
  "update_billing",
  "cancel_order",
  "create_ticket",
  "escalate",
  "notify_customer",
  "update_subscription",
  "manual_review",
];

export function HumanFeedback({ actionId, actionType, operatorId }: Props) {
  const [open, setOpen] = useState(false);
  const [score, setScore] = useState(3);
  const [correctAction, setCorrectAction] = useState("");
  const [notes, setNotes] = useState("");
  const qc = useQueryClient();

  const mutation = useMutation({
    mutationFn: (payload: HumanFeedbackPayload) =>
      submitFeedback(actionId, payload),
    onSuccess: () => {
      toast.success("Feedback recorded — thank you!");
      setOpen(false);
      setNotes("");
      qc.invalidateQueries({ queryKey: ["actions"] });
    },
    onError: (err: Error) => {
      toast.error(`Feedback failed: ${err.message}`);
    },
  });

  const handleSubmit = () => {
    mutation.mutate({
      operator_id: operatorId,
      quality_score: score,
      correct_action: correctAction || undefined,
      feedback_notes: notes || undefined,
    });
  };

  return (
    <div className="mt-2">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs text-amber-400 hover:text-amber-300 transition-colors"
      >
        <span className="text-base">{open ? "▾" : "▸"}</span>
        Correct AI Decision
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="mt-2 rounded-lg bg-amber-950/30 border border-amber-800/50 p-3 space-y-3">
              <p className="text-xs font-semibold text-amber-300">
                Human Feedback — AI proposed: <em>{actionType}</em>
              </p>

              {/* Quality score */}
              <div>
                <label className="text-xs text-gray-400 block mb-1">Quality score</label>
                <div className="flex gap-2">
                  {[1, 2, 3, 4, 5].map((n) => (
                    <button
                      key={n}
                      onClick={() => setScore(n)}
                      className={`w-8 h-8 rounded-full text-xs font-bold transition-all ${
                        score === n
                          ? "bg-amber-500 text-gray-900"
                          : "bg-gray-800 text-gray-400 hover:bg-gray-700"
                      }`}
                    >
                      {n}
                    </button>
                  ))}
                  <span className="text-xs text-gray-500 self-center ml-1">
                    {["", "Wrong", "Poor", "OK", "Good", "Excellent"][score]}
                  </span>
                </div>
              </div>

              {/* Correct action */}
              <div>
                <label className="text-xs text-gray-400 block mb-1">
                  Correct action (optional)
                </label>
                <select
                  value={correctAction}
                  onChange={(e) => setCorrectAction(e.target.value)}
                  className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1.5 text-xs text-gray-200 focus:outline-none focus:border-amber-500"
                >
                  <option value="">— same as proposed —</option>
                  {ACTION_OPTIONS.filter((a) => a !== actionType).map((a) => (
                    <option key={a} value={a}>{a}</option>
                  ))}
                </select>
              </div>

              {/* Notes */}
              <div>
                <label className="text-xs text-gray-400 block mb-1">Notes (optional)</label>
                <textarea
                  value={notes}
                  onChange={(e) => setNotes(e.target.value)}
                  rows={2}
                  placeholder="e.g. Customer has VIP flag — should always escalate"
                  className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1.5 text-xs text-gray-200 resize-none focus:outline-none focus:border-amber-500"
                />
              </div>

              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => setOpen(false)}
                  className="px-3 py-1 text-xs text-gray-400 hover:text-gray-200 transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={mutation.isPending}
                  className="px-4 py-1.5 bg-amber-600 hover:bg-amber-500 text-white text-xs font-semibold rounded transition-colors disabled:opacity-50"
                >
                  {mutation.isPending ? "Sending…" : "Submit Feedback"}
                </button>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
