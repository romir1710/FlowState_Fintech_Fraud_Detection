'use client';

/**
 * page.tsx — FlowState Real-Time Fraud Monitor
 *
 * Connects to the backend WebSocket server and renders two raw lists:
 *   - Flagged Transactions (isFraud === true)
 *   - Approved Transactions (isFraud === false)
 *
 * INTENTIONALLY UNSTYLED — UI templates will be applied separately.
 *
 * WebSocket URL resolution (in priority order):
 *   1. NEXT_PUBLIC_WS_URL env var (set at build time or via Docker at runtime)
 *   2. Fallback: ws://localhost:8080 for local development
 *
 * The client implements automatic reconnection with a 3-second delay.
 * Each list is capped at MAX_LIST_SIZE items (FIFO) to prevent memory growth.
 */

import { useEffect, useState, useRef, useCallback } from 'react';
import { ProcessedTransaction, WsMessage } from '../types/transaction';

// ─── Config ──────────────────────────────────────────────────────────────────
/**
 * Resolve the WebSocket URL.
 *   - In production / Docker: set NEXT_PUBLIC_WS_URL at build time or as a
 *     runtime env var when using Next.js standalone output.
 *   - In local development: falls back to ws://localhost:8080.
 */
const WS_URL: string =
  process.env.NEXT_PUBLIC_WS_URL ||
  'ws://localhost:8080';

/** Maximum number of entries to keep in each list before oldest are dropped. */
const MAX_LIST_SIZE = 50;

/** Reconnection delay in milliseconds. */
const RECONNECT_DELAY_MS = 3_000;

// ─── Types ───────────────────────────────────────────────────────────────────
type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'error';

// ─── Component ───────────────────────────────────────────────────────────────
export default function HomePage() {
  const [approved, setApproved] = useState<ProcessedTransaction[]>([]);
  const [flagged, setFlagged] = useState<ProcessedTransaction[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>('connecting');
  const [messageCount, setMessageCount] = useState(0);

  // Ref so the cleanup function in useEffect always has the latest socket
  const wsRef = useRef<WebSocket | null>(null);
  // Ref to track if the component is still mounted (prevents state updates after unmount)
  const mountedRef = useRef(true);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;

    console.log(`[FlowState] Connecting to WebSocket: ${WS_URL}`);
    setStatus('connecting');

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      if (!mountedRef.current) return;
      setStatus('connected');
      console.log('[FlowState] ✅ WebSocket connected.');
    };

    ws.onmessage = (event: MessageEvent<string>) => {
      if (!mountedRef.current) return;

      let parsed: WsMessage;
      try {
        parsed = JSON.parse(event.data) as WsMessage;
      } catch {
        console.warn('[FlowState] Received unparseable message:', event.data);
        return;
      }

      // Filter out handshake messages — only process transaction payloads
      if ('type' in parsed && parsed.type === 'connected') {
        console.log('[FlowState] Handshake received:', parsed.message);
        return;
      }

      const tx = parsed as ProcessedTransaction;

      setMessageCount((prev) => prev + 1);

      if (tx.isFraud) {
        // Prepend newest fraud tx; cap list size (FIFO drop from end)
        setFlagged((prev) => [tx, ...prev].slice(0, MAX_LIST_SIZE));
      } else {
        // Prepend newest approved tx; cap list size (FIFO drop from end)
        setApproved((prev) => [tx, ...prev].slice(0, MAX_LIST_SIZE));
      }
    };

    ws.onerror = () => {
      if (!mountedRef.current) return;
      setStatus('error');
      console.error('[FlowState] WebSocket error.');
    };

    ws.onclose = (event) => {
      if (!mountedRef.current) return;
      setStatus('disconnected');
      console.log(
        `[FlowState] WebSocket closed (code: ${event.code}). Reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`,
      );
      setTimeout(connect, RECONNECT_DELAY_MS);
    };
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      wsRef.current?.close();
    };
  }, [connect]);

  // ─── Render ────────────────────────────────────────────────────────────────
  return (
    <div>
      <h1>FlowState — Real-Time Fraud Monitor</h1>

      {/* ── Connection Status Bar ── */}
      <p>
        WebSocket URL: <code>{WS_URL}</code>
        {' | '}
        Status: <strong>{status.toUpperCase()}</strong>
        {' | '}
        Total messages received: <strong>{messageCount}</strong>
      </p>

      <hr />

      {/* ── Flagged Transactions ── */}
      <section id="flagged-transactions">
        <h2>
          🚨 Flagged Transactions (FRAUD) — {flagged.length}
          {flagged.length === MAX_LIST_SIZE && ` (showing latest ${MAX_LIST_SIZE})`}
        </h2>

        {flagged.length === 0 ? (
          <p>No fraud detected yet. Waiting for stream...</p>
        ) : (
          <ul>
            {flagged.map((tx) => (
              <li key={tx.transactionId}>
                <pre>{JSON.stringify(tx, null, 2)}</pre>
              </li>
            ))}
          </ul>
        )}
      </section>

      <hr />

      {/* ── Approved Transactions ── */}
      <section id="approved-transactions">
        <h2>
          ✅ Approved Transactions — {approved.length}
          {approved.length === MAX_LIST_SIZE && ` (showing latest ${MAX_LIST_SIZE})`}
        </h2>

        {approved.length === 0 ? (
          <p>No approved transactions yet. Waiting for stream...</p>
        ) : (
          <ul>
            {approved.map((tx) => (
              <li key={tx.transactionId}>
                <pre>{JSON.stringify(tx, null, 2)}</pre>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
