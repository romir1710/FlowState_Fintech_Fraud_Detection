/**
 * transaction.ts — Shared TypeScript types for the FlowState frontend.
 * Must be kept in sync with backend/src/types.ts.
 */

/** Raw transaction as produced by the backend Kafka producer. */
export interface Transaction {
  transactionId: string;
  userId: string;
  amount: number;
  timestamp: string;
  location: string;
}

/**
 * Enriched transaction broadcast by the consumer via WebSocket.
 * All fields from Transaction plus fraud analysis results.
 */
export interface ProcessedTransaction extends Transaction {
  /** Composite risk score 0–100. Formula: amountScore(0-65) + velocityScore(0-35) */
  riskScore: number;
  /** true if riskScore > FRAUD_THRESHOLD (default 75) */
  isFraud: boolean;
  /** Number of transactions this userId made in the last 60s (Redis fixed window) */
  velocity: number;
}

/** Handshake message sent by the WebSocket server on connect. */
export interface WsHandshake {
  type: 'connected';
  message: string;
}

/** Union of all possible incoming WebSocket messages. */
export type WsMessage = ProcessedTransaction | WsHandshake;
