import type {
  ActionProposal,
  DlqEvent,
  EngagementLog,
  HumanFeedbackPayload,
  TraceTimeline,
} from "../types";

const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api";
const API_KEY = import.meta.env.VITE_API_KEY ?? "";

export function getStoredToken(): string | null {
  return localStorage.getItem("synapse_jwt");
}

export function setStoredToken(token: string | null): void {
  if (token) localStorage.setItem("synapse_jwt", token);
  else localStorage.removeItem("synapse_jwt");
}

function headers(): HeadersInit {
  const token = getStoredToken();
  return {
    "Content-Type": "application/json",
    ...(API_KEY ? { "X-API-Key": API_KEY } : {}),
    ...(token ? { "Authorization": `Bearer ${token}` } : {}),
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

// ── Auth ──────────────────────────────────────────────────────────────────────

export interface LoginResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
}

export const loginOperator = async (username: string, password: string): Promise<LoginResponse> => {
  const body = new URLSearchParams({ username, password });
  const res = await fetch(`${BASE}/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${text}`);
  }
  return res.json() as Promise<LoginResponse>;
};

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

// ── DLQ ───────────────────────────────────────────────────────────────────────

export const fetchDlqEvents = (status: "OPEN" | "ACKNOWLEDGED" | "ALL" = "OPEN") =>
  request<DlqEvent[]>(`/dlq?status=${status}`);

export const acknowledgeDlqEvent = (outboxId: string, operatorId: string) =>
  request(`/dlq/${outboxId}/acknowledge`, {
    method: "POST",
    body: JSON.stringify({ operator_id: operatorId }),
  });

// ── System ────────────────────────────────────────────────────────────────────

export const fetchSystemStatus = () =>
  request<Record<string, unknown>>("/system/status");

export const fetchQueueDepths = () =>
  request<Record<string, number>>("/actions/depths");
