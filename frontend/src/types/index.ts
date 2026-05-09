// ── Action proposal ────────────────────────────────────────────────────────────

export type ActionStatus =
  | "PENDING"
  | "APPROVED"
  | "REJECTED"
  | "EXPIRED"
  | "STALE"
  | "ESCALATED_STALE"
  | "EXECUTING"
  | "EXECUTED"
  | "FAILED";

export interface ActionProposal {
  id: string;
  correlation_id: string;
  order_id: string | null;
  customer_id: string | null;
  action_type: string;
  action_description: string;
  action_payload: Record<string, unknown>;
  predicted_before_state: Record<string, unknown>;
  predicted_after_state: Record<string, unknown>;
  status: ActionStatus;
  version: number;
  created_at: string;
  updated_at: string;
  expires_at: string;
  approved_by: string | null;
  rejection_reason: string | null;
  retry_count: number;
  executed_at: string | null;
  execution_error: string | null;
}

// ── Timeline / Trace ───────────────────────────────────────────────────────────

export type NodeType =
  | "AI_REASONING"
  | "ORACLE_WRITE"
  | "DLQ_FAILURE"
  | "OPERATOR_HITL"
  | "ENGAGEMENT"
  | "ENGAGEMENT_SENT"
  | "CUSTOMER_REPLIED";

export interface TraceEvent {
  event_id: string;
  trace_id: string;
  node_type: NodeType;
  event_type: string;
  timestamp: string;
  label: string;
  detail: Record<string, unknown>;
}

export interface TraceTimeline {
  trace_id: string;
  event_count: number;
  events: TraceEvent[];
}

// ── Engagement ─────────────────────────────────────────────────────────────────

export type SentimentLabel = "POSITIVE" | "NEUTRAL" | "NEGATIVE" | "ANGRY";

export interface EngagementLog {
  id: string;
  action_id: string;
  correlation_id: string;
  customer_id: string | null;
  channel: string;
  template_used: string;
  message_body: string;
  recipient_masked: string;
  sent_at: string | null;
  status: string;
  customer_response: string;
  response_text: string | null;
  response_received_at: string | null;
  sentiment_score: number | null;
  sentiment_label: SentimentLabel | null;
  created_at: string;
}

// ── NOC WebSocket events ───────────────────────────────────────────────────────

export type NocEventType =
  | "resolution_proposed"
  | "action_approved"
  | "force_approved"
  | "dlq_acknowledged"
  | "system_stale"
  | "system_healthy"
  | "force_sync"
  | "sentiment_alert"
  | "heartbeat";

export interface NocEvent {
  event_type: NocEventType;
  opco_id: string;
  timestamp: string;
  trace_id?: string;
  correlation_id?: string;
  customer_id?: number | string | null;
  engagement_id?: string;
  sentiment_score?: number;
  sentiment_label?: SentimentLabel;
  channel?: string;
  connections?: number;
  [key: string]: unknown;
}

// ── Human feedback ────────────────────────────────────────────────────────────

export interface HumanFeedbackPayload {
  operator_id: string;
  quality_score: number;
  correct_action?: string;
  feedback_notes?: string;
}
