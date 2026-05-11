import { useState } from "react";
import { motion } from "framer-motion";
import { loginOperator, setStoredToken } from "../api/client";

interface Props {
  onLogin: (operatorId: string, role: string, opcoId: string | null) => void;
}

function decodeJwtPayload(token: string): Record<string, unknown> {
  try {
    const base64 = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(base64));
  } catch {
    return {};
  }
}

export function LoginPage({ onLogin }: Props) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError]       = useState<string | null>(null);
  const [loading, setLoading]   = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!username.trim() || !password.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await loginOperator(username.trim(), password);
      setStoredToken(resp.access_token);

      const payload = decodeJwtPayload(resp.access_token);
      const operatorId = String(payload.sub ?? username);
      const role       = String(payload.role ?? "operator");
      const opcoId     = payload.opco_id ? String(payload.opco_id) : null;

      localStorage.setItem("synapse_operator_id", operatorId);
      localStorage.setItem("synapse_role", role);
      if (opcoId) localStorage.setItem("synapse_opco_id", opcoId);

      onLogin(operatorId, role, opcoId);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#0f0a1e] relative overflow-hidden">
      {/* Background orbs */}
      <div className="absolute inset-0 pointer-events-none">
        <div
          className="absolute rounded-full"
          style={{
            width: 700, height: 700,
            background: "radial-gradient(circle, rgba(109,40,217,0.30) 0%, transparent 70%)",
            top: -260, left: -200,
            filter: "blur(80px)",
          }}
        />
        <div
          className="absolute rounded-full"
          style={{
            width: 550, height: 550,
            background: "radial-gradient(circle, rgba(192,132,252,0.20) 0%, transparent 70%)",
            bottom: -180, right: -180,
            filter: "blur(80px)",
          }}
        />
      </div>

      <motion.div
        initial={{ opacity: 0, y: 24, scale: 0.97 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
        className="relative z-10 w-full max-w-sm"
      >
        <div className="glass rounded-3xl border border-violet-500/15 p-8 shadow-2xl shadow-violet-950/50">
          {/* Logo */}
          <div className="flex flex-col items-center gap-3 mb-8">
            <motion.div
              initial={{ scale: 0.5, rotate: -20 }}
              animate={{ scale: 1, rotate: 0 }}
              transition={{ type: "spring", stiffness: 260, damping: 20, delay: 0.15 }}
              className="w-14 h-14 rounded-2xl bg-gradient-to-br from-violet-500 to-purple-700 flex items-center justify-center text-2xl font-black shadow-lg shadow-violet-500/40"
            >
              S
            </motion.div>
            <div className="text-center">
              <h1 className="text-xl font-bold tracking-tight gradient-text leading-none">
                Synapse NOC
              </h1>
              <p className="text-xs text-gray-500 mt-1">Telco Fallout AI Platform</p>
            </div>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <label className="text-xs font-semibold text-gray-400 uppercase tracking-widest">
                Operator ID
              </label>
              <input
                type="text"
                autoComplete="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="noc-operator"
                className="bg-black/30 border border-white/8 focus:border-violet-500/50 rounded-xl px-3.5 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none transition-colors"
              />
            </div>

            <div className="flex flex-col gap-1.5">
              <label className="text-xs font-semibold text-gray-400 uppercase tracking-widest">
                Password
              </label>
              <input
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                className="bg-black/30 border border-white/8 focus:border-violet-500/50 rounded-xl px-3.5 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none transition-colors"
              />
            </div>

            {error && (
              <motion.p
                initial={{ opacity: 0, y: -6 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-xs text-red-400 bg-red-500/8 border border-red-500/20 rounded-xl px-3 py-2"
              >
                {error}
              </motion.p>
            )}

            <button
              type="submit"
              disabled={loading || !username.trim() || !password.trim()}
              className="mt-1 py-2.5 bg-gradient-to-r from-violet-600 to-purple-700 hover:from-violet-500 hover:to-purple-600 text-white text-sm font-bold rounded-xl transition-all duration-200 disabled:opacity-40 shadow-lg shadow-violet-900/30"
            >
              {loading ? "Authenticating…" : "Sign In"}
            </button>
          </form>

          <p className="mt-5 text-center text-[11px] text-gray-600">
            Dev mode: any username + <span className="font-mono text-gray-500">synapse-dev</span>
          </p>

          {/* Role legend */}
          <div className="mt-4 pt-4 border-t border-white/5 space-y-1.5 text-[10px] text-gray-600">
            <div className="flex items-start gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-violet-400 inline-block mt-0.5 shrink-0" />
              <span>MANAGER — Full Execution + Global Regional Access</span>
            </div>
            <div className="flex items-start gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-gray-500 inline-block mt-0.5 shrink-0" />
              <span>OPERATOR — Full Execution + Restricted to Assigned Region</span>
            </div>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
