"""
Kafka topic names and partition configuration for Synapse.

Topic strategy:
  oracle.cdc.all_changes    — Raw CDC events from every Oracle table insert/update
  ai.orchestration.tasks    — Agent analysis jobs (replaces RabbitMQ resolve_jobs)
  legacy.commands.writeback — Approved write instructions destined for Oracle
  legacy.commands.dlq       — Phase 10: Dead Letter Queue for failed Oracle write-backs
  synapse.system.events     — Phase 11: SYSTEM_STALE, health signals, HIGH_SIGNAL prefetch
  synapse.canary.results    — Phase 12: per-resolution canary scoring for model comparison
"""

TOPIC_CDC_ALL_CHANGES  = "oracle.cdc.all_changes"
TOPIC_AI_ORCHESTRATION = "ai.orchestration.tasks"
TOPIC_WRITEBACK        = "legacy.commands.writeback"
TOPIC_DLQ              = "legacy.commands.dlq"
TOPIC_SYSTEM_EVENTS    = "synapse.system.events"
TOPIC_CANARY_RESULTS   = "synapse.canary.results"   # Phase 12: canary scoring stream

# Partition count sized for horizontal scale; replication_factor=1 for local dev.
# In production: replication_factor >= 3, min.insync.replicas = 2.
TOPIC_CONFIGS: dict[str, dict] = {
    TOPIC_CDC_ALL_CHANGES:  {"num_partitions": 6, "replication_factor": 1},
    TOPIC_AI_ORCHESTRATION: {"num_partitions": 3, "replication_factor": 1},
    TOPIC_WRITEBACK:        {"num_partitions": 3, "replication_factor": 1},
    TOPIC_DLQ:              {"num_partitions": 3, "replication_factor": 1},
    TOPIC_SYSTEM_EVENTS:    {"num_partitions": 1, "replication_factor": 1},
    TOPIC_CANARY_RESULTS:   {"num_partitions": 2, "replication_factor": 1},
}

# Consumer group IDs
GROUP_HYDRATOR        = "synapse-hydrator"
GROUP_THINKER         = "synapse-thinker"
GROUP_ORACLE_ADAPTER  = "synapse-oracle-adapter"
GROUP_DLQ_MONITOR     = "synapse-dlq-monitor"
GROUP_PREFETCH        = "synapse-prefetch"    # Phase 12: predictive hydration
GROUP_CANARY          = "synapse-canary"      # Phase 12: canary analysis consumer
