/**
 * websocket-server.ts — WebSocket Broadcast Server
 *
 * Creates a combined HTTP + WebSocket server on a single port.
 * HTTP handles Render health checks; WebSocket handles real-time
 * transaction broadcasts to connected frontend clients.
 *
 * Port resolution (in priority order):
 *   1. process.env.PORT   — injected by Render for its exposed port
 *   2. process.env.WS_PORT — local dev override
 *   3. 8080               — local dev default
 */

import * as http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { ProcessedTransaction } from './types';

// ─── Config ──────────────────────────────────────────────────────────────────
// Render injects PORT; WS_PORT is a local-dev alias kept for backwards compat.
const PORT = parseInt(process.env.PORT ?? process.env.WS_PORT ?? '8080', 10);

// ─── Module-level singletons ─────────────────────────────────────────────────
let wss: WebSocketServer | null = null;
let httpServer: http.Server | null = null;

// ─── Init ─────────────────────────────────────────────────────────────────────
/**
 * Creates a combined HTTP + WebSocket server:
 *   - GET /health → 200 JSON (keeps Render web service alive)
 *   - WS upgrade  → real-time broadcast channel
 *
 * Idempotent — safe to call multiple times.
 */
export function initWebSocketServer(): WebSocketServer {
  if (wss) return wss;

  // Shared HTTP server — Render proxies both HTTP and WS upgrades through it
  httpServer = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
    const clients = wss?.clients.size ?? 0;
    if (req.url === '/health' || req.url === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'flowstate-consumer', connectedClients: clients }));
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
    }
  });

  // Attach WS server to the existing HTTP server (shared port)
  wss = new WebSocketServer({ server: httpServer });

  wss.on('connection', (socket: WebSocket, request: http.IncomingMessage) => {
    const clientIp = request.socket.remoteAddress ?? 'unknown';
    console.log(`[WebSocket] Client connected: ${clientIp} | Total clients: ${wss!.clients.size}`);

    // Send handshake confirmation to newly connected client
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'connected', message: 'FlowState stream active' }));
    }

    socket.on('close', (code: number, reason: Buffer) => {
      console.log(`[WebSocket] Client disconnected: ${clientIp} (code: ${code}, reason: ${reason.toString() || 'none'}) | Remaining: ${wss!.clients.size}`);
    });

    socket.on('error', (err: Error) => {
      console.error(`[WebSocket] Socket error from ${clientIp}:`, err.message);
    });
  });

  wss.on('error', (err: Error) => {
    console.error('[WebSocket] Server error:', err.message);
  });

  httpServer.listen(PORT, () => {
    console.log(`[WebSocket] ✅ Server listening on port ${PORT} (HTTP /health + WS upgrade)`);
  });

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
 * Gracefully closes the WebSocket server and HTTP server.
 */
export function closeWebSocketServer(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!wss) return resolve();
    wss.close((err?: Error) => {
      if (err) return reject(err);
      console.log('[WebSocket] Server closed.');
      wss = null;
      httpServer?.close(() => {
        httpServer = null;
        resolve();
      });
    });
  });
}
