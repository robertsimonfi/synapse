import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { ActionProposal } from "../types";

interface Props {
  action: ActionProposal;
}

function buildChainOfThought(action: ActionProposal): string[] {
  const lines: string[] = [];
  const payload = action.action_payload as Record<string, unknown>;

  if (payload.confidence)
    lines.push(`Confidence: ${payload.confidence}`);

  const before = action.predicted_before_state;
  if (before) {
    const entries = Object.entries(before).slice(0, 4);
    if (entries.length)
      lines.push(`Current state: ${entries.map(([k, v]) => `${k}=${v}`).join(", ")}`);
  }

  const after = action.predicted_after_state;
  if (after) {
    const entries = Object.entries(after).slice(0, 4);
    if (entries.length)
      lines.push(`Predicted after: ${entries.map(([k, v]) => `${k}=${v}`).join(", ")}`);
  }

  if (payload.reasoning) lines.push(String(payload.reasoning));
  if (payload.rationale) lines.push(String(payload.rationale));
  if (payload.risk_level) lines.push(`Risk level: ${payload.risk_level}`);
  if (payload.policy_checks)
    lines.push(`Policy checks: ${JSON.stringify(payload.policy_checks)}`);

  if (!lines.length)
    lines.push(`Action type "${action.action_type}" was proposed based on the current customer state and runbook analysis.`);

  return lines;
}

export function AIThoughtPanel({ action }: Props) {
  const [open, setOpen] = useState(false);
  const thoughts = buildChainOfThought(action);

  return (
    <div className="mt-1">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs text-blue-400/70 hover:text-blue-300 transition-colors group"
      >
        <motion.span
          animate={{ rotate: open ? 90 : 0 }}
          transition={{ duration: 0.18 }}
          className="text-blue-500/60 group-hover:text-blue-400 transition-colors"
        >
          ▸
        </motion.span>
        AI Thought Process
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.22 }}
            className="overflow-hidden"
          >
            <div className="mt-2 rounded-xl bg-blue-950/20 border border-blue-500/12 p-3 space-y-1.5 backdrop-blur-sm">
              <p className="text-[10px] font-bold text-blue-400/70 uppercase tracking-widest mb-2">
                Chain of Thought — {action.action_type}
              </p>
              {thoughts.map((line, i) => (
                <div key={i} className="flex gap-2 text-xs text-gray-400">
                  <span className="text-blue-500/50 shrink-0">→</span>
                  <span>{line}</span>
                </div>
              ))}
              <div className="pt-1.5 border-t border-blue-500/10 mt-2">
                <p className="text-[10px] text-gray-600">
                  Correlation:{" "}
                  <span className="font-mono text-gray-500">{action.correlation_id}</span>
                </p>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
