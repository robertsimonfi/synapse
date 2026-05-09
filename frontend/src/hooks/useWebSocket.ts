import { useCallback, useEffect, useRef, useState } from "react";
import type { NocEvent } from "../types";

type Handler = (evt: NocEvent) => void;

interface UseWebSocketOptions {
  opcoId: string;
  onEvent?: Handler;
  enabled?: boolean;
}

const WS_BASE =
  typeof window !== "undefined"
    ? `${window.location.protocol === "https:" ? "wss" : "ws"}://${
        import.meta.env.VITE_WS_HOST ?? window.location.host
      }`
    : "";

export function useWebSocket({ opcoId, onEvent, enabled = true }: UseWebSocketOptions) {
  const ws = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState<NocEvent | null>(null);
  const handlerRef = useRef<Handler | undefined>(onEvent);
  handlerRef.current = onEvent;

  const connect = useCallback(() => {
    if (!enabled || !opcoId) return;

    const url = `${WS_BASE}/ws/noc/${opcoId}`;
    const socket = new WebSocket(url);
    ws.current = socket;

    socket.onopen = () => setConnected(true);

    socket.onmessage = (msg) => {
      try {
        const evt: NocEvent = JSON.parse(msg.data as string);
        setLastEvent(evt);
        handlerRef.current?.(evt);
      } catch {
        // malformed frame — ignore
      }
    };

    socket.onclose = () => {
      setConnected(false);
      // exponential back-off up to 30 s
      const delay = Math.min(30_000, 2_000 + Math.random() * 3_000);
      reconnectTimer.current = setTimeout(connect, delay);
    };

    socket.onerror = () => socket.close();
  }, [enabled, opcoId]);

  useEffect(() => {
    connect();
    return () => {
      ws.current?.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };
  }, [connect]);

  return { connected, lastEvent };
}
