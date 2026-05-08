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
