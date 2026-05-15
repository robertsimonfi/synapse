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
          "50%":       { boxShadow: "0 0 0 8px rgba(239,68,68,0)" },
        },
        "fade-up": {
          from: { opacity: "0", transform: "translateY(18px)" },
          to:   { opacity: "1", transform: "translateY(0)" },
        },
        "fade-in": {
          from: { opacity: "0" },
          to:   { opacity: "1" },
        },
        "glow-pulse": {
          "0%, 100%": { opacity: "0.6" },
          "50%":      { opacity: "1" },
        },
      },
      animation: {
        shake:          "shake 0.6s cubic-bezier(.36,.07,.19,.97) both",
        "pulse-border": "pulse-border 1.5s ease-in-out infinite",
        "fade-up":      "fade-up 0.45s ease-out both",
        "fade-in":      "fade-in 0.5s ease-out both",
        "glow-pulse":   "glow-pulse 2s ease-in-out infinite",
      },
      colors: {
        // Synapse brand palette
        "synapse-purple": "#6D28D9",
        "synapse-dark":   "#0F172A",
        // Timeline node colours
        "node-ai":      "#3B82F6",
        "node-oracle":  "#22C55E",
        "node-dlq":     "#EF4444",
        "node-hitl":    "#F59E0B",
        "node-engage":  "#8B5CF6",
        "node-neutral": "#6B7280",
      },
    },
  },
  plugins: [],
};
