"""
Prometheus metrics for the Synapse action pipeline.

Counters track lifecycle events; the histogram measures operator response latency
(time from action creation to human approval) and execution latency (approval → executed).
"""

from prometheus_client import Counter, Gauge, Histogram

actions_pending_total = Counter(
    "synapse_actions_pending_total",
    "Total actions entering PENDING state (one per proposed action)",
    ["action_type"],
)

actions_approved_total = Counter(
    "synapse_actions_approved_total",
    "Total actions approved by human operators",
    ["action_type"],
)

actions_rejected_total = Counter(
    "synapse_actions_rejected_total",
    "Total actions rejected by human operators",
    ["action_type"],
)

actions_expired_total = Counter(
    "synapse_actions_expired_total",
    "Total PENDING actions auto-expired after exceeding the review window",
)

actions_failed_total = Counter(
    "synapse_actions_failed_total",
    "Total actions that failed execution after exhausting all retries",
    ["action_type"],
)

execution_latency_seconds = Histogram(
    "synapse_actions_execution_latency_seconds",
    "Time from APPROVED status to EXECUTED status (seconds)",
    ["action_type"],
    buckets=[1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600],
)

pending_queue_depth = Gauge(
    "synapse_pending_queue_depth",
    "Current number of actions sitting in PENDING state awaiting human review",
)

# ── Phase 11: Kafka consumer lag & PII masking ────────────────────────────────

kafka_consumer_lag_seconds = Gauge(
    "synapse_kafka_consumer_lag_seconds",
    "Estimated consumer lag in seconds (producer timestamp → consumed timestamp) per topic-group",
    ["topic", "group_id"],
)

kafka_circuit_breaker_stale = Gauge(
    "synapse_kafka_circuit_breaker_stale",
    "1 when the hydrator circuit breaker is SYSTEM_STALE (lag > threshold), 0 when HEALTHY",
    ["topic"],
)

pii_masked_fields_total = Counter(
    "synapse_pii_masked_fields_total",
    "Total number of PII field values tokenised before MongoDB ODS write",
    ["field_type"],
)

# ── Phase 12: Predictive prefetch ─────────────────────────────────────────────

prefetch_signals_received_total = Counter(
    "synapse_prefetch_signals_received_total",
    "HIGH_SIGNAL events received by the predictive hydration engine",
    ["signal_type", "opco_id"],
)

prefetch_customers_queued_total = Counter(
    "synapse_prefetch_customers_queued_total",
    "Subscribers queued for proactive re-hydration following a HIGH_SIGNAL event",
    ["signal_type", "opco_id"],
)

force_sync_total = Counter(
    "synapse_force_sync_total",
    "Operator-triggered force-sync calls that bypass the Kafka pipeline",
    ["opco_id"],
)

# ── Phase 12: Canary analysis ─────────────────────────────────────────────────

canary_success_rate = Gauge(
    "synapse_canary_success_rate",
    "Rolling approval rate for a given model version (0.0 – 1.0)",
    ["model_version"],
)

canary_sample_count = Gauge(
    "synapse_canary_sample_count",
    "Total resolutions scored within the canary evaluation window",
    ["model_version"],
)

canary_promotion_total = Counter(
    "synapse_canary_promotion_total",
    "Number of times a candidate model was promoted to champion",
    ["from_model", "to_model"],
)

# ── Phase 13: Tiered Storage ──────────────────────────────────────────────────

archived_documents_total = Counter(
    "synapse_archived_documents_total",
    "Subscriber documents moved from hot ODS to cold store by the archiver",
    ["opco_id"],
)

cold_store_documents_gauge = Gauge(
    "synapse_cold_store_documents",
    "Current number of cold-tier subscriber stubs in the hot ODS",
)

cold_promotions_total = Counter(
    "synapse_cold_promotions_total",
    "Subscriber documents promoted from cold store to hot ODS by the Hydrator",
    ["opco_id", "reason"],
)

# ── Phase 13: Oracle HA Circuit Breaker ──────────────────────────────────────

oracle_circuit_breaker_open = Gauge(
    "synapse_oracle_circuit_breaker_open",
    "1 when the Oracle primary circuit breaker is OPEN, 0 when CLOSED/HALF_OPEN",
)

oracle_standby_reads_total = Counter(
    "synapse_oracle_standby_reads_total",
    "Read operations routed to the Oracle standby replica (circuit breaker OPEN)",
)

oracle_writes_queued_total = Counter(
    "synapse_oracle_writes_queued_total",
    "Write commands queued to Kafka because Oracle primary is unavailable",
    ["opco_id"],
)

# ── Phase 13: AI Token Budget ─────────────────────────────────────────────────

token_budget_remaining_gauge = Gauge(
    "synapse_token_budget_remaining",
    "Tokens remaining in the current billing period for an OpCo",
    ["opco_id"],
)

token_budget_consumed_total = Counter(
    "synapse_token_budget_consumed_total",
    "Cumulative tokens consumed (input + output) per OpCo",
    ["opco_id"],
)

token_quota_exceeded_total = Counter(
    "synapse_token_quota_exceeded_total",
    "Number of times an OpCo's monthly quota was exhausted",
    ["opco_id"],
)

# ── Defensive Execution: drift / failure / force-approve ──────────────────────

drift_detections_total = Counter(
    "synapse_drift_detections_total",
    "Pre-flight drift detections that aborted execution (APPROVED → STALE)",
    ["opco_id", "action_type"],
)

handler_failures_total = Counter(
    "synapse_handler_failures_total",
    "Actions that failed after exhausting all handler retry attempts",
    ["action_type"],
)

force_approved_total = Counter(
    "synapse_force_approved_total",
    "Actions approved via Force Approve, bypassing pre-flight drift checks",
    ["operator", "action_type"],
)
