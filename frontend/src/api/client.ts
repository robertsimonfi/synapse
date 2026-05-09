import type {
  ActionProposal,
  EngagementLog,
  HumanFeedbackPayload,
  TraceTimeline,
} from "../types";

const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api";
const API_KEY = import.meta.env.VITE_API_KEY ?? "";

function headers(): HeadersInit {
  return {
    "Content-Type": "application/json",
    ...(API_KEY ? { "X-API-Key": API_KEY } : {}),
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { ...headers(), ...(init?.headers ?? {}) },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

// ── Actions ───────────────────────────────────────────────────────────────────

export const fetchActions = (status?: string) =>
  request<ActionProposal[]>(
    `/actions${status ? `?status=${status}` : ""}`,
  );

export const fetchAction = (id: string) =>
  request<ActionProposal>(`/actions/${id}`);

export const approveAction = (id: string, operator_id: string) =>
  request(`/actions/${id}/approve`, {
    method: "POST",
    body: JSON.stringify({ operator_id }),
  });

export const rejectAction = (id: string, operator_id: string, reason: string) =>
  request(`/actions/${id}/reject`, {
    method: "POST",
    body: JSON.stringify({ operator_id, reason }),
  });

export const submitFeedback = (id: string, payload: HumanFeedbackPayload) =>
  request(`/actions/${id}/feedback`, {
    method: "POST",
    body: JSON.stringify(payload),
  });

// ── Timeline ──────────────────────────────────────────────────────────────────

export const fetchTrace = (traceId: string) =>
  request<TraceTimeline>(`/system/trace/${traceId}`);

// ── Engagements ───────────────────────────────────────────────────────────────

export const fetchEngagements = (actionId?: string) =>
  request<EngagementLog[]>(
    `/engagements${actionId ? `?action_id=${actionId}` : ""}`,
  );

// ── System ────────────────────────────────────────────────────────────────────

export const fetchSystemStatus = () =>
  request<Record<string, unknown>>("/system/status");

export const fetchQueueDepths = () =>
  request<Record<string, number>>("/actions/depths");
