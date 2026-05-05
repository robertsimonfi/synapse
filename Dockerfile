# syntax=docker/dockerfile:1.7

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for native wheels (chromadb pulls in onnxruntime, tokenizers, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Install Python deps into an isolated venv we copy into the runtime image.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    APP_HOME=/app \
    DATA_DIR=/data \
    # ChromaDB's embedding func writes ONNX models under $HOME/.cache. Point it
    # at the PVC so (a) the non-root `app` user has write perms and (b) the
    # ~25MB model survives pod restarts instead of re-downloading every time.
    HOME=/data \
    XDG_CACHE_HOME=/data/.cache

# Runtime libs only (no compilers). libgomp1 is needed by onnxruntime (chromadb).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r app && useradd -r -g app -d ${APP_HOME} -s /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv

WORKDIR ${APP_HOME}

# Copy application source
COPY --chown=app:app agent/   ./agent/
COPY --chown=app:app api/     ./api/
COPY --chown=app:app rag/     ./rag/
COPY --chown=app:app db/      ./db/
COPY --chown=app:app docker/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p ${DATA_DIR}/chromadb ${DATA_DIR}/agent_memory ${DATA_DIR}/.cache \
    && chown -R app:app ${DATA_DIR}

# Persistent state lives outside the image — mount a volume here in compose/prod.
VOLUME ["/data"]

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
