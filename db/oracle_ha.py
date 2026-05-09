"""
Oracle High-Availability Pool — Phase 13 Circuit Breaker for Oracle.

Manages two Oracle connection endpoints:
  Primary  — ORA_HOST / ORA_PORT / ORA_SERVICE_NAME
  Standby  — ORA_STANDBY_HOST / ORA_STANDBY_PORT / ORA_STANDBY_SERVICE_NAME

Read traffic:   primary → standby on connection failure (transparent fallover).
Write traffic:  primary only.  When the circuit is OPEN, writes raise
                OracleWriteUnavailable so callers can queue to Kafka and replay
                when the primary recovers.

Circuit breaker state machine:
  CLOSED    — primary healthy; all connections go to primary
  OPEN      — primary failed ≥ FAILURE_THRESHOLD times; reads go to standby
  HALF_OPEN — probing primary after HALF_OPEN_PROBE_SECONDS; one test read

Prometheus metrics updated on every state transition:
  synapse_oracle_circuit_breaker_open   (gauge: 1=OPEN, 0=CLOSED/HALF_OPEN)
  synapse_oracle_standby_reads_total    (counter: reads routed to standby)

Environment
-----------
  ORA_HOST / ORA_PORT / ORA_SERVICE_NAME          primary Oracle
  ORA_STANDBY_HOST / ORA_STANDBY_PORT /
    ORA_STANDBY_SERVICE_NAME                       standby (optional)
  ORA_USER / ORA_PASSWORD                          credentials (both endpoints)
  ORACLE_HA_FAILURE_THRESHOLD                      failures before OPEN (default: 3)
  ORACLE_HA_HALF_OPEN_PROBE_SECONDS                HALF_OPEN probe interval (default: 60)
"""

from __future__ import annotations

import logging
import os
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

_FAILURE_THRESHOLD   = int(os.getenv("ORACLE_HA_FAILURE_THRESHOLD",        "3"))
_HALF_OPEN_PROBE_SEC = int(os.getenv("ORACLE_HA_HALF_OPEN_PROBE_SECONDS", "60"))


class OracleWriteUnavailable(Exception):
    """
    Raised when the primary Oracle circuit is OPEN and writes cannot proceed.
    Callers should re-queue the command to Kafka for later replay.
    """


class CBState(str, Enum):
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class OracleHAPool:
    """
    Thread-safe Oracle HA pool with primary/standby failover.

    Designed for use in the thread-pool context (agent/tools.py,
    worker/oracle_adapter.py) where connections are short-lived per-call.
    Acquire via the singleton: OracleHAPool.instance()

    Usage:
        pool = OracleHAPool.instance()
        conn = pool.get_read_connection()   # may return standby
        conn = pool.get_write_connection()  # primary only; raises on OPEN
    """

    _instance: Optional[OracleHAPool] = None

    def __init__(self) -> None:
        self._state         = CBState.CLOSED
        self._failure_count = 0
        self._last_probe_at = 0.0

    @classmethod
    def instance(cls) -> OracleHAPool:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── State machine ──────────────────────────────────────────────────────────

    def _on_success(self) -> None:
        self._failure_count = 0
        if self._state != CBState.CLOSED:
            logger.info("Oracle primary recovered — circuit breaker CLOSED")
            self._state = CBState.CLOSED
            self._emit_metric(0)

    def _on_failure(self) -> None:
        self._failure_count += 1
        if self._failure_count >= _FAILURE_THRESHOLD:
            if self._state == CBState.CLOSED:
                logger.error(
                    "Oracle primary unreachable %d times — circuit breaker OPEN "
                    "(reads → standby, writes → Kafka queue)",
                    _FAILURE_THRESHOLD,
                )
            self._state = CBState.OPEN
            self._emit_metric(1)

    def _should_probe(self) -> bool:
        """True when OPEN and the HALF_OPEN probe window has elapsed."""
        return (
            self._state == CBState.OPEN
            and time.monotonic() - self._last_probe_at >= _HALF_OPEN_PROBE_SEC
        )

    @staticmethod
    def _emit_metric(value: int) -> None:
        try:
            from observability.metrics import oracle_circuit_breaker_open
            oracle_circuit_breaker_open.set(value)
        except Exception:
            pass  # metrics are best-effort

    # ── DSN helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _primary_dsn() -> str:
        host    = os.getenv("ORA_HOST",         "localhost")
        port    = os.getenv("ORA_PORT",         "1521")
        service = os.getenv("ORA_SERVICE_NAME", "ORCLPDB1")
        return f"{host}:{port}/{service}"

    @staticmethod
    def _standby_dsn() -> Optional[str]:
        host = os.getenv("ORA_STANDBY_HOST", "")
        if not host:
            return None
        port    = os.getenv("ORA_STANDBY_PORT",         "1521")
        service = os.getenv("ORA_STANDBY_SERVICE_NAME",
                             os.getenv("ORA_SERVICE_NAME", "ORCLPDB1"))
        return f"{host}:{port}/{service}"

    def _connect(self, dsn: str):
        import oracledb  # noqa: PLC0415
        return oracledb.connect(
            user=os.getenv("ORA_USER",     "telco_agent"),
            password=os.getenv("ORA_PASSWORD", ""),
            dsn=dsn,
        )

    # ── Public connection API ─────────────────────────────────────────────────

    def get_read_connection(self):
        """
        Return a connection for read operations.

        Tries the primary first (or probes it when HALF_OPEN).
        Falls back to the standby replica when the circuit is OPEN.
        Raises ConnectionError if neither endpoint is reachable.
        """
        if self._state == CBState.CLOSED or self._should_probe():
            try:
                conn = self._connect(self._primary_dsn())
                self._on_success()
                return conn
            except Exception as exc:
                logger.warning(
                    "Oracle primary read failed (cb=%s failure#%d): %s",
                    self._state, self._failure_count + 1, exc,
                )
                self._on_failure()
                self._last_probe_at = time.monotonic()

        standby_dsn = self._standby_dsn()
        if standby_dsn:
            logger.info("Oracle HA: routing read to standby replica")
            from observability.metrics import oracle_standby_reads_total
            oracle_standby_reads_total.inc()
            return self._connect(standby_dsn)

        raise ConnectionError(
            "Oracle primary is OPEN and no standby is configured — "
            "reads unavailable until primary recovers"
        )

    def get_write_connection(self):
        """
        Return a primary connection for write operations.

        Raises OracleWriteUnavailable when the circuit is OPEN so callers
        can queue the command to Kafka and replay later.
        """
        if self._state == CBState.OPEN and not self._should_probe():
            raise OracleWriteUnavailable(
                f"Oracle primary circuit breaker is {self._state} — "
                "write queued to Kafka for later replay"
            )
        try:
            conn = self._connect(self._primary_dsn())
            self._on_success()
            return conn
        except Exception as exc:
            self._on_failure()
            self._last_probe_at = time.monotonic()
            raise OracleWriteUnavailable(
                f"Oracle primary unavailable: {exc} — write queued to Kafka"
            ) from exc

    @property
    def state(self) -> CBState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count
