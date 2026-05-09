/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      keyframes: {
        shake: {
          "0%, 100%": { transform: "translateX(0)" },
          "10%, 50%, 90%": { transform: "translateX(-6px)" },
          "30%, 70%": { transform: "translateX(6px)" },
        },
        "pulse-border": {
          "0%, 100%": { boxShadow: "0 0 0 0 rgba(239,68,68,0.7)" },
          "50%": { boxShadow: "0 0 0 8px rgba(239,68,68,0)" },
        },
      },
      animation: {
        shake: "shake 0.6s cubic-bezier(.36,.07,.19,.97) both",
        "pulse-border": "pulse-border 1.5s ease-in-out infinite",
      },
      colors: {
        // Node colours for the timeline
        "node-ai":       "#3B82F6", // blue-500
        "node-oracle":   "#22C55E", // green-500
        "node-dlq":      "#EF4444", // red-500
        "node-hitl":     "#F59E0B", // amber-500
        "node-engage":   "#8B5CF6", // violet-500
        "node-neutral":  "#6B7280", // gray-500
      },
    },
  },
  plugins: [],
};
