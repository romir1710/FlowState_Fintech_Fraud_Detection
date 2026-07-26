/**
 * websocket-server.ts — WebSocket Broadcast Server
 *
 * Initializes a WebSocket server and exports a `broadcast()` function
 * that the consumer calls to push every processed transaction to all
 * connected frontend clients in real-time.
 *
 * Configuration (environment variables):
 *   WS_PORT   WebSocket server port (default: 8080)
 */

import { WebSocketServer, WebSocket } from 'ws';
import { ProcessedTransaction } from './types';

// ─── Config ──────────────────────────────────────────────────────────────────
const WS_PORT = parseInt(process.env.WS_PORT ?? '8080', 10);

// ─── Module-level singleton ───────────────────────────────────────────────────
let wss: WebSocketServer | null = null;

// ─── Init ─────────────────────────────────────────────────────────────────────
/**
 * Creates and starts the WebSocket server. Must be called once before
 * `broadcast()` is used. Idempotent — safe to call multiple times.
 */
export function initWebSocketServer(): WebSocketServer {
  if (wss) return wss;

  wss = new WebSocketServer({ port: WS_PORT });

  wss.on('connection', (socket: WebSocket, request) => {
    const clientIp = request.socket.remoteAddress ?? 'unknown';
    console.log(`[WebSocket] Client connected: ${clientIp} | Total clients: ${wss!.clients.size}`);

    // Send a handshake confirmation to the newly connected client
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'connected', message: 'FlowState stream active' }));
    }

    socket.on('close', (code, reason) => {
      console.log(`[WebSocket] Client disconnected: ${clientIp} (code: ${code}, reason: ${reason.toString() || 'none'}) | Remaining: ${wss!.clients.size}`);
    });

    socket.on('error', (err: Error) => {
      console.error(`[WebSocket] Socket error from ${clientIp}:`, err.message);
    });
  });

  wss.on('error', (err: Error) => {
    console.error('[WebSocket] Server error:', err.message);
  });

  console.log(`[WebSocket] ✅ Server listening on ws://localhost:${WS_PORT}`);
  return wss;
}

// ─── Broadcast ────────────────────────────────────────────────────────────────
/**
 * Serializes and sends a processed transaction to every connected client.
 * Silently skips clients that are not in OPEN state.
 */
export function broadcast(transaction: ProcessedTransaction): void {
  if (!wss || wss.clients.size === 0) return;

  const payload = JSON.stringify(transaction);

  wss.clients.forEach((client: WebSocket) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload, (err?: Error) => {
        if (err) {
          console.error('[WebSocket] Failed to send to a client:', err.message);
        }
      });
    }
  });
}

// ─── Shutdown ────────────────────────────────────────────────────────────────
/**
 * Gracefully closes the WebSocket server and all client connections.
 */
export function closeWebSocketServer(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!wss) return resolve();
    wss.close((err) => {
      if (err) return reject(err);
      console.log('[WebSocket] Server closed.');
      wss = null;
      resolve();
    });
  });
}
