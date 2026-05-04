#!/usr/bin/env sh
# Container entrypoint.
# - Optionally seeds SQLite + ingests runbooks on first boot when AUTO_SEED / AUTO_INGEST=true.
# - Then execs the CMD (uvicorn by default).
set -eu

DATA_DIR="${DATA_DIR:-/data}"
mkdir -p "${DATA_DIR}"

# ── SQLite seed (only when DB_TYPE=sqlite and the file is missing) ────────────
if [ "${DB_TYPE:-sqlite}" = "sqlite" ] && [ "${AUTO_SEED:-false}" = "true" ]; then
    SQLITE_FILE="${SQLITE_PATH:-/data/telco_dev.db}"
    if [ ! -f "${SQLITE_FILE}" ]; then
        echo "[entrypoint] No SQLite DB at ${SQLITE_FILE} — seeding..."
        python -m db.setup_sqlite
    else
        echo "[entrypoint] SQLite DB already present at ${SQLITE_FILE} — skipping seed."
    fi
fi

# ── RAG ingest (idempotent — knowledge_base.ingest_runbooks() skips if populated) ──
if [ "${AUTO_INGEST:-false}" = "true" ]; then
    echo "[entrypoint] Ensuring runbooks are ingested into ChromaDB..."
    python -m rag.knowledge_base || echo "[entrypoint] WARN: ingest failed (continuing)"
fi

echo "[entrypoint] Starting: $*"
exec "$@"
