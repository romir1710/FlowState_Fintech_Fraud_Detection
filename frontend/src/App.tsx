/**
 * App.tsx — FlowState Real-Time Fraud Monitor
 *
 * Full-screen dark-themed fraud monitoring interface with:
 *   - Live WebSocket transaction stream from the backend consumer
 *   - Cursor-following spotlight that reveals a second background image
 *   - Filter toggle: All | Approved | Flagged
 *   - Premium glassmorphic transaction cards
 *
 * WebSocket URL resolution:
 *   1. VITE_WS_URL env var (set in Vercel)
 *   2. Fallback: ws://localhost:8080 for local dev
 */

import { useEffect, useState, useRef, useCallback } from 'react';
import { Shield, ShieldAlert, ShieldCheck } from 'lucide-react';
import RevealLayer from './components/RevealLayer';
import { ProcessedTransaction, WsMessage } from './types/transaction';

// ─── Config ──────────────────────────────────────────────────────────────────

const WS_URL: string = import.meta.env.VITE_WS_URL || 'ws://localhost:8080';
const MAX_LIST_SIZE = 50;
const RECONNECT_DELAY_MS = 3_000;
const SPOTLIGHT_R = 260;

const BG_IMAGE_1 =
  'https://images.higgs.ai/?default=1&output=webp&url=https%3A%2F%2Fd8j0ntlcm91z4.cloudfront.net%2Fuser_38xzZboKViGWJOttwIXH07lWA1P%2Fhf_20260609_195923_b0ba8ace-1d1d-4f2c-9a28-1ab84b330680.png&w=1280&q=85';
const BG_IMAGE_2 =
  'https://images.higgs.ai/?default=1&output=webp&url=https%3A%2F%2Fd8j0ntlcm91z4.cloudfront.net%2Fuser_38xzZboKViGWJOttwIXH07lWA1P%2Fhf_20260609_201152_bba90a12-bf12-459f-91f0-51f237dbaf3b.png&w=1280&q=85';

// ─── Types ───────────────────────────────────────────────────────────────────

type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'error';
type FilterMode = 'All' | 'Approved' | 'Flagged';

// ─── Transaction Card ────────────────────────────────────────────────────────

function TransactionCard({ tx }: { tx: ProcessedTransaction }) {
  const fields: { label: string; value: string }[] = [
    { label: 'Transaction ID', value: tx.transactionId },
    { label: 'User', value: tx.userId },
    { label: 'Amount', value: `$${tx.amount.toFixed(2)}` },
    { label: 'Location', value: tx.location },
    { label: 'Velocity', value: `${tx.velocity} tx/min` },
    { label: 'Risk Score', value: `${tx.riskScore}/100` },
    { label: 'Timestamp', value: new Date(tx.timestamp).toLocaleTimeString() },
  ];

  return (
    <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-5 flex flex-col gap-3 transition-all hover:bg-black/50 pointer-events-auto">
      {/* Status indicator */}
      <div className="flex items-center gap-2">
        <span
          className={`w-2 h-2 rounded-full ${tx.isFraud ? 'bg-red-500' : 'bg-emerald-500'}`}
        />
        <span
          className={`text-xs font-semibold uppercase tracking-wider ${
            tx.isFraud ? 'text-red-400' : 'text-emerald-400'
          }`}
        >
          {tx.isFraud ? 'Fraud Detected' : 'Approved'}
        </span>
      </div>
      {/* Key-value pairs */}
      {fields.map((f) => (
        <div key={f.label} className="flex items-center justify-between">
          <span className="text-white/50 text-xs uppercase tracking-wider">{f.label}</span>
          <span className="text-white text-sm font-medium">{f.value}</span>
        </div>
      ))}
    </div>
  );
}

// ─── Stream Column ───────────────────────────────────────────────────────────

function StreamColumn({
  title,
  icon,
  transactions,
  accentColor,
  displayCount,
}: {
  title: string;
  icon: React.ReactNode;
  transactions: ProcessedTransaction[];
  accentColor: string;
  displayCount: string;
}) {
  return (
    <div className="flex flex-col gap-4 h-full">
      <div className="flex items-center gap-2 pointer-events-none">
        {icon}
        <h2 className={`text-lg font-semibold ${accentColor}`}>{title}</h2>
        <span className="text-white/40 text-sm ml-auto">{displayCount}</span>
      </div>
      <div className="overflow-y-auto pr-2 scrollbar-hide flex flex-col gap-3 flex-1">
        {transactions.length === 0 ? (
          <p className="text-white/30 text-sm italic">Waiting for stream…</p>
        ) : (
          transactions.map((tx) => <TransactionCard key={tx.transactionId} tx={tx} />)
        )}
      </div>
    </div>
  );
}

// ─── App ─────────────────────────────────────────────────────────────────────

export default function App() {
  // ── Transaction state ─────────────────────────────────────────────────────
  const [approved, setApproved] = useState<ProcessedTransaction[]>([]);
  const [flagged, setFlagged] = useState<ProcessedTransaction[]>([]);
  const [_status, setStatus] = useState<ConnectionStatus>('connecting');
  const [_messageCount, setMessageCount] = useState(0);
  const [totalApproved, setTotalApproved] = useState(0);
  const [totalFlagged, setTotalFlagged] = useState(0);
  const [filter, setFilter] = useState<FilterMode>('All');

  // ── WebSocket refs ────────────────────────────────────────────────────────
  const wsRef = useRef<WebSocket | null>(null);
  const mountedRef = useRef(true);

  // ── Cursor tracking refs ──────────────────────────────────────────────────
  const mouse = useRef({ x: -999, y: -999 });
  const smooth = useRef({ x: -999, y: -999 });
  const rafRef = useRef<number>(0);
  const [cursorPos, setCursorPos] = useState({ x: -999, y: -999 });

  // ── WebSocket connection (identical logic to original) ────────────────────
  const connect = useCallback(() => {
    if (!mountedRef.current) return;
    setStatus('connecting');

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      if (!mountedRef.current) return;
      setStatus('connected');
    };

    ws.onmessage = (event: MessageEvent<string>) => {
      if (!mountedRef.current) return;

      let parsed: WsMessage;
      try {
        parsed = JSON.parse(event.data) as WsMessage;
      } catch {
        return;
      }

      // Filter out handshake messages
      if ('type' in parsed && parsed.type === 'connected') return;

      const tx = parsed as ProcessedTransaction;
      setMessageCount((prev) => prev + 1);

      if (tx.isFraud) {
        setTotalFlagged((prev) => prev + 1);
        setFlagged((prev) => [tx, ...prev].slice(0, MAX_LIST_SIZE));
      } else {
        setTotalApproved((prev) => prev + 1);
        setApproved((prev) => [tx, ...prev].slice(0, MAX_LIST_SIZE));
      }
    };

    ws.onerror = () => {
      if (!mountedRef.current) return;
      setStatus('error');
    };

    ws.onclose = () => {
      if (!mountedRef.current) return;
      setStatus('disconnected');
      setTimeout(connect, RECONNECT_DELAY_MS);
    };
  }, []);

  // ── Mount: WebSocket + wake-up ping + cursor tracking ─────────────────────
  useEffect(() => {
    mountedRef.current = true;
    connect();

    // Wake up producer on Render (prevents free-tier sleep)
    fetch('https://flowstate-producer.onrender.com/health', { mode: 'no-cors' }).catch(() => {});

    // Global mouse tracking
    const handleMouseMove = (e: MouseEvent) => {
      mouse.current.x = e.clientX;
      mouse.current.y = e.clientY;
    };
    window.addEventListener('mousemove', handleMouseMove);

    // RAF smoothing loop
    const loop = () => {
      smooth.current.x += (mouse.current.x - smooth.current.x) * 0.1;
      smooth.current.y += (mouse.current.y - smooth.current.y) * 0.1;
      setCursorPos({ x: smooth.current.x, y: smooth.current.y });
      rafRef.current = requestAnimationFrame(loop);
    };
    rafRef.current = requestAnimationFrame(loop);

    return () => {
      mountedRef.current = false;
      wsRef.current?.close();
      window.removeEventListener('mousemove', handleMouseMove);
      cancelAnimationFrame(rafRef.current);
    };
  }, [connect]);

  // ── Filter buttons ────────────────────────────────────────────────────────
  const filters: FilterMode[] = ['All', 'Approved', 'Flagged'];

  const getFilterIcon = (f: FilterMode) => {
    switch (f) {
      case 'All':
        return <Shield size={14} />;
      case 'Approved':
        return <ShieldCheck size={14} />;
      case 'Flagged':
        return <ShieldAlert size={14} />;
    }
  };

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div
      className="min-h-screen bg-black tracking-[-0.02em]"
      style={{ fontFamily: "'Inter', sans-serif" }}
    >
      <section
        className="relative w-full overflow-hidden h-screen bg-black flex flex-col items-center"
        style={{ height: '100dvh' }}
      >
        {/* ── Base image (z-10) ── */}
        <div
          className="absolute inset-0 bg-center bg-cover bg-no-repeat z-10 hero-zoom"
          style={{ backgroundImage: `url(${BG_IMAGE_1})` }}
        />

        {/* ── Reveal layer (z-30) ── */}
        <RevealLayer
          image={BG_IMAGE_2}
          cursorX={cursorPos.x}
          cursorY={cursorPos.y}
          spotlightRadius={SPOTLIGHT_R}
        />

        {/* ── Navigation bar (z-[100]) ── */}
        <nav className="fixed top-6 left-1/2 -translate-x-1/2 bg-white/10 backdrop-blur-md border border-white/20 rounded-full p-1.5 flex items-center gap-1 z-[100]">
          {filters.map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-6 py-2 rounded-full text-sm font-medium flex items-center gap-1.5 transition-colors ${
                filter === f
                  ? 'text-white bg-white/20'
                  : 'text-white/70 hover:text-white hover:bg-white/10'
              }`}
            >
              {getFilterIcon(f)}
              {f}
            </button>
          ))}
        </nav>

        {/* ── Heading (z-50) ── */}
        <div className="relative mt-[12vh] flex flex-col items-center text-center px-5 pointer-events-none z-50">
          <h1 className="text-white leading-[0.95]">
            <span
              className="block font-playfair italic font-normal text-5xl sm:text-7xl md:text-8xl hero-anim hero-reveal"
              style={{ letterSpacing: '-0.05em', animationDelay: '0.25s' }}
            >
              FlowState
            </span>
            <span
              className="block font-normal text-2xl sm:text-4xl md:text-5xl mt-3 text-white/80 hero-anim hero-reveal"
              style={{ letterSpacing: '-0.04em', animationDelay: '0.42s' }}
            >
              Fraud Detection
            </span>
          </h1>

          {/* Total counter — only in All view */}
          {filter === 'All' && (totalApproved + totalFlagged) > 0 && (
            <p className="mt-4 text-white/40 text-sm font-medium tracking-wide">
              Total Transactions:{' '}
              <span className="text-white/70">{totalApproved + totalFlagged}</span>
            </p>
          )}
        </div>

        {/* ── Transaction streams (z-50) ── */}
        <div
          className="relative w-full max-w-6xl mx-auto mt-10 px-6 h-[60vh] flex gap-8 z-50 hero-anim hero-fade"
          style={{ animationDelay: '0.7s' }}
        >
          {filter === 'All' && (
            <>
              <div className="w-1/2 flex flex-col gap-4">
                <StreamColumn
                  title="Approved Transactions"
                  icon={<ShieldCheck size={18} className="text-emerald-400" />}
                  transactions={approved}
                  accentColor="text-emerald-400"
                  displayCount={totalApproved > MAX_LIST_SIZE ? `${MAX_LIST_SIZE}+` : String(totalApproved)}
                />
              </div>
              <div className="w-1/2 flex flex-col gap-4">
                <StreamColumn
                  title="Flagged (Fraud)"
                  icon={<ShieldAlert size={18} className="text-red-400" />}
                  transactions={flagged}
                  accentColor="text-red-400"
                  displayCount={totalFlagged > MAX_LIST_SIZE ? `${MAX_LIST_SIZE}+` : String(totalFlagged)}
                />
              </div>
            </>
          )}

          {filter === 'Approved' && (
            <div className="max-w-2xl mx-auto w-full flex flex-col gap-4">
              <StreamColumn
                title="Approved Transactions"
                icon={<ShieldCheck size={18} className="text-emerald-400" />}
                transactions={approved}
                accentColor="text-emerald-400"
                displayCount={String(totalApproved)}
              />
            </div>
          )}

          {filter === 'Flagged' && (
            <div className="max-w-2xl mx-auto w-full flex flex-col gap-4">
              <StreamColumn
                title="Flagged (Fraud)"
                icon={<ShieldAlert size={18} className="text-red-400" />}
                transactions={flagged}
                accentColor="text-red-400"
                displayCount={String(totalFlagged)}
              />
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
