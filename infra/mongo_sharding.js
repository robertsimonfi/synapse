/**
 * Synapse — Phase 12: MongoDB Sharding Advisory Script
 *
 * Run this against a mongos router when promoting from a standalone MongoDB
 * instance to a sharded cluster.  Idempotent: safe to re-run.
 *
 * Prerequisites
 * -------------
 * 1. A 3-shard replica-set cluster is provisioned (e.g. Atlas M30+ or AKS with
 *    the MongoDB operator).
 * 2. This script is executed by a user with clusterAdmin + dbAdmin on "synapse".
 *
 * Shard key rationale
 * -------------------
 * { opco_id: 1, customer_id: 1 }
 *
 *   • opco_id   — high-cardinality prefix that pins an entire OpCo's data to a
 *                 bounded set of chunks, keeping cross-shard scatter-gather to zero
 *                 for the most common query pattern (get subscriber by opco).
 *   • customer_id — guarantees monotonically increasing key within each opco bucket,
 *                   avoiding hot-spot chunks on insert-heavy workloads.
 *
 * Zone mapping (optional)
 * -----------------------
 * Assign each opco_id to a zone co-located with its regional data centre to satisfy
 * data-residency requirements (GDPR, PECR).  Uncomment and adjust the zone blocks
 * below once zones are tagged on your shards.
 *
 * Usage
 *   mongosh "mongodb+srv://<user>:<pass>@<mongos-host>/admin" --file mongo_sharding.js
 */

// ── Enable sharding on the database ──────────────────────────────────────────
sh.enableSharding("synapse");
print("✓ Sharding enabled on database: synapse");

// ── subscribers collection ────────────────────────────────────────────────────
// The compound index (opco_id, customer_id) must exist BEFORE shardCollection()
// is called.  ensure_ods_indexes() in db/ods.py creates it on startup.
sh.shardCollection(
    "synapse.subscribers",
    { opco_id: 1, customer_id: 1 },
    /* unique= */ false,
    /* options= */ { numInitialChunks: 12 }   // 2 chunks per opco per shard on a 3-shard cluster
);
print("✓ synapse.subscribers sharded on { opco_id: 1, customer_id: 1 }");

// ── outbox collection ─────────────────────────────────────────────────────────
sh.shardCollection(
    "synapse.outbox",
    { opco_id: 1, created_at: 1 }
);
print("✓ synapse.outbox sharded on { opco_id: 1, created_at: 1 }");

// ── dlq_events collection ─────────────────────────────────────────────────────
sh.shardCollection(
    "synapse.dlq_events",
    { opco_id: 1, failed_at: 1 }
);
print("✓ synapse.dlq_events sharded on { opco_id: 1, failed_at: 1 }");

// ── canary_runs (not sharded — low volume, single-primary is fine) ─────────────
print("ℹ  synapse.canary_runs: not sharded (low-write, analysis workload)");

// ── Optional: Zone mapping for data residency ─────────────────────────────────
//
// Tag your shards in Atlas or via sh.addShardTag(), then uncomment:
//
// sh.addTagRange(
//     "synapse.subscribers",
//     { opco_id: "VODAFONE_UK",  customer_id: MinKey },
//     { opco_id: "VODAFONE_UK",  customer_id: MaxKey },
//     "eu-west-1"
// );
// sh.addTagRange(
//     "synapse.subscribers",
//     { opco_id: "VODAFONE_DE",  customer_id: MinKey },
//     { opco_id: "VODAFONE_DE",  customer_id: MaxKey },
//     "eu-central-1"
// );
// sh.addTagRange(
//     "synapse.subscribers",
//     { opco_id: "VODAFONE_ES",  customer_id: MinKey },
//     { opco_id: "VODAFONE_ES",  customer_id: MaxKey },
//     "eu-south-1"
// );

// ── Verification ──────────────────────────────────────────────────────────────
print("\n── Shard distribution ──");
printjson(db.adminCommand({ listShards: 1 }));
print("\n── Chunk counts ──");
printjson(db.getSiblingDB("config").chunks.aggregate([
    { $group: { _id: "$ns", chunks: { $sum: 1 } } },
    { $sort:  { _id: 1 } }
]).toArray());
