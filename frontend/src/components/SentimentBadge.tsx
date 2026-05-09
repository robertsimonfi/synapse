import { clsx } from "clsx";
import type { SentimentLabel } from "../types";

interface Props {
  label: SentimentLabel | null | undefined;
  score?: number | null;
}

const cfg: Record<SentimentLabel, { text: string; className: string; emoji: string }> = {
  POSITIVE: { text: "Positive",  className: "bg-green-900 text-green-300 border-green-700",   emoji: "😊" },
  NEUTRAL:  { text: "Neutral",   className: "bg-gray-800 text-gray-300 border-gray-600",     emoji: "😐" },
  NEGATIVE: { text: "Negative",  className: "bg-orange-900 text-orange-300 border-orange-700", emoji: "😠" },
  ANGRY:    { text: "ANGRY",     className: "bg-red-900 text-red-300 border-red-600 font-bold", emoji: "🔴" },
};

export function SentimentBadge({ label, score }: Props) {
  if (!label) return null;
  const { text, className, emoji } = cfg[label];
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border",
        className,
      )}
    >
      <span>{emoji}</span>
      {text}
      {score !== null && score !== undefined && (
        <span className="opacity-60">({score > 0 ? "+" : ""}{score.toFixed(2)})</span>
      )}
    </span>
  );
}
