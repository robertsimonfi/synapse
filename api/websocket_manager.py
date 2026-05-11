"""
WebSocket connection manager for the real-time NOC dashboard — Phase 11.

Clients subscribe to  /ws/noc/{opco_id}  to receive live JSON events scoped
to their operational boundary.  "ALL" gives NOC super-users a cross-opco view.

Event schema
────────────
{
  "event":    "resolution_proposed" | "action_approved" | "action_rejected"
              | "force_approved" | "dlq_acknowledged" | "system_stale"
              | "system_healthy" | "heartbeat",
  "opco_id":  "<OPCO>",
  "ts":       "<ISO-8601 UTC>",
  ...event-specific fields...
}
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import DefaultDict

from fastapi import WebSocket

VALID_OPCO_IDS = {"UK_REGION", "DE_REGION", "ES_REGION", "IT_REGION", "DEFAULT"}


class ConnectionManager:
    def __init__(self) -> None:
        # opco_id → active WebSocket connections
        self._connections: DefaultDict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, opco_id: str) -> None:
        await websocket.accept()
        self._connections[opco_id].append(websocket)

    def disconnect(self, websocket: WebSocket, opco_id: str) -> None:
        conns = self._connections.get(opco_id, [])
        try:
            conns.remove(websocket)
        except ValueError:
            pass

    async def broadcast(self, opco_id: str, event: dict) -> None:
        """
        Push *event* to all clients for *opco_id* and all "ALL" super-user subscribers.
        Silently drops dead connections.
        """
        targets = list(self._connections.get(opco_id, []))
        if opco_id != "ALL":
            targets += list(self._connections.get("ALL", []))

        # deduplicate
        seen: set[int] = set()
        unique = [ws for ws in targets if id(ws) not in seen and not seen.add(id(ws))]  # type: ignore[func-returns-value]

        dead: list[tuple[WebSocket, str]] = []
        payload = json.dumps(event, default=str)
        for ws in unique:
            try:
                await ws.send_text(payload)
            except Exception:
                for oc, sockets in self._connections.items():
                    if ws in sockets:
                        dead.append((ws, oc))
                        break

        for ws, oc in dead:
            self.disconnect(ws, oc)

    def connection_count(self, opco_id: str | None = None) -> int:
        if opco_id:
            return len(self._connections.get(opco_id, []))
        return sum(len(v) for v in self._connections.values())

    def connected_opcos(self) -> list[str]:
        return [oc for oc, sockets in self._connections.items() if sockets]


def noc_event(event_type: str, opco_id: str, **kwargs) -> dict:
    return {
        "event":   event_type,
        "opco_id": opco_id,
        "ts":      datetime.now(timezone.utc).isoformat(),
        **kwargs,
    }


# Module-level singleton
manager = ConnectionManager()
