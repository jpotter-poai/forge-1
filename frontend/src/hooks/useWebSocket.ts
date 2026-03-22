import { useCallback, useEffect, useRef } from "react";
import { openExecutionSocket } from "@/api/client";
import type { WsMessage } from "@/types/pipeline";

interface UseWebSocketOptions {
  onMessage: (msg: WsMessage) => void;
  onClose?: () => void;
}

export function useWebSocket(options: UseWebSocketOptions) {
  const { onMessage, onClose } = options;
  const wsRef = useRef<WebSocket | null>(null);
  const onMessageRef = useRef(onMessage);
  const onCloseRef = useRef(onClose);

  // Keep refs current without re-attaching listeners
  useEffect(() => {
    onMessageRef.current = onMessage;
  }, [onMessage]);
  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  const connect = useCallback((pipelineId: string) => {
    // Close any existing socket
    if (wsRef.current) {
      wsRef.current.close();
    }

    const ws = openExecutionSocket(pipelineId);
    wsRef.current = ws;

    ws.onmessage = (event: MessageEvent) => {
      try {
        const msg = JSON.parse(event.data as string) as WsMessage;
        onMessageRef.current(msg);
      } catch {
        // non-JSON frame — ignore
      }
    };

    ws.onclose = () => {
      onCloseRef.current?.();
    };

    ws.onerror = () => {
      ws.close();
    };
  }, []);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
  }, []);

  // Clean up on unmount
  useEffect(() => {
    return () => {
      wsRef.current?.close();
    };
  }, []);

  return { connect, disconnect };
}
