import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const API_TARGET =
  process.env.VITE_API_TARGET ?? "http://localhost:8000";

const proxyConfig = {
  "/api": {
    target: API_TARGET,
    changeOrigin: true,
    rewrite: (path: string) => path.replace(/^\/api/, ""),
  },
  "/ws": {
    target: API_TARGET.replace(/^http/, "ws"),
    ws: true,
  },
};

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 3000,
    proxy: proxyConfig,
  },
  preview: {
    host: "0.0.0.0",
    port: 3000,
    proxy: proxyConfig,
  },
});
