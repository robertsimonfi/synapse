import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { ActionProposal } from "../types";

interface Props {
  action: ActionProposal;
}

const COT_STEPS: { key: string; label: string; color: string }[] = [
  { key: "observation",      label: "Observation",      color: "text-sky-400" },
  { key: "constraint_check", label: "Constraint Check", color: "text-amber-400" },
  { key: "strategy",         label: "Strategy",         color: "text-violet-400" },
  { key: "recommendation",   label: "Recommendation",   color: "text-emerald-400" },
];

function buildFallbackThoughts(action: ActionProposal): string[] {
  const lines: string[] = [];
  const payload = action.action_payload as Record<string, unknown>;

  if (payload.confidence) lines.push(`Confidence: ${payload.confidence}`);

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

  if (!lines.length)
    lines.push(
      `Action "${action.action_type}" was proposed based on current customer state and runbook analysis.`,
    );

  return lines;
}

export function AIThoughtPanel({ action }: Props) {
  const [open, setOpen] = useState(false);
  const cot = (action as ActionProposal & { chain_of_thought?: Record<string, string> | null })
    .chain_of_thought;
  const hasStructuredCot = cot && typeof cot === "object" && Object.keys(cot).length > 0;
  const fallbackLines = hasStructuredCot ? [] : buildFallbackThoughts(action);

  return (
    <div className="mt-1">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs text-violet-400/70 hover:text-violet-300 transition-colors group"
      >
        <motion.span
          animate={{ rotate: open ? 90 : 0 }}
          transition={{ duration: 0.18 }}
          className="text-violet-500/60 group-hover:text-violet-400 transition-colors"
        >
          ▸
        </motion.span>
        AI Thought Process
        {hasStructuredCot && (
          <span className="ml-1 text-[9px] px-1.5 py-0.5 bg-violet-500/12 text-violet-400/80 border border-violet-500/20 rounded-full uppercase tracking-wider">
            Full CoT
          </span>
        )}
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
            <div className="mt-2 rounded-xl bg-violet-950/20 border border-violet-500/12 p-3 space-y-2.5 backdrop-blur-sm">
              <p className="text-[10px] font-bold text-violet-400/70 uppercase tracking-widest">
                Chain of Thought — {action.action_type}
              </p>

              {hasStructuredCot && cot["natural_language_summary"] && (
                <div className="rounded-lg bg-violet-500/8 border border-violet-500/18 px-3 py-2.5">
                  <p className="text-[10px] font-bold text-violet-400/60 uppercase tracking-widest mb-1">
                    AI Summary
                  </p>
                  <p className="text-sm text-gray-200 leading-relaxed">
                    {cot["natural_language_summary"]}
                  </p>
                </div>
              )}

              {hasStructuredCot ? (
                COT_STEPS.map(({ key, label, color }) => {
                  const text = cot[key];
                  if (!text) return null;
                  return (
                    <div key={key} className="flex gap-2.5 text-xs">
                      <div className="shrink-0 pt-0.5">
                        <span className={`font-bold text-[10px] uppercase tracking-widest ${color}`}>
                          {label}
                        </span>
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-gray-300 leading-relaxed">{text}</p>
                      </div>
                    </div>
                  );
                })
              ) : (
                fallbackLines.map((line, i) => (
                  <div key={i} className="flex gap-2 text-xs text-gray-400">
                    <span className="text-violet-500/50 shrink-0">→</span>
                    <span>{line}</span>
                  </div>
                ))
              )}

              <div className="pt-1.5 border-t border-violet-500/10 mt-2">
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
