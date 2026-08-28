import { useEffect, useRef, useState } from "react";
import type { LiveEvent } from "./api";

export interface SecondPoint {
  t: number; // unix seconds
  label: string; // "mm:ss" for the axis
  allow: number;
  reject: number;
}

export interface LiveState {
  series: SecondPoint[]; // last `window` seconds, oldest first
  totalAllowed: number;
  totalRejected: number;
  connected: boolean;
  lastKey: string | null;
}

const WINDOW = 60;

// useLiveSeries subscribes to the SSE decision stream and rolls it up into a
// per-second allow/reject series. The chart re-renders once a second (not once
// per event) so a burst of thousands of events stays cheap and the time axis
// keeps advancing while idle.
export function useLiveSeries(): LiveState {
  const buckets = useRef<Map<number, { allow: number; reject: number }>>(new Map());
  const totals = useRef({ allowed: 0, rejected: 0 });
  const lastKey = useRef<string | null>(null);
  const [state, setState] = useState<LiveState>({
    series: emptySeries(),
    totalAllowed: 0,
    totalRejected: 0,
    connected: false,
    lastKey: null,
  });

  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onopen = () => setState((s) => ({ ...s, connected: true }));
    es.onerror = () => setState((s) => ({ ...s, connected: false }));
    es.onmessage = (m) => {
      let ev: LiveEvent;
      try {
        ev = JSON.parse(m.data);
      } catch {
        return;
      }
      const sec = Math.floor(ev.tsUnixMs / 1000);
      const b = buckets.current.get(sec) ?? { allow: 0, reject: 0 };
      if (ev.allowed) {
        b.allow++;
        totals.current.allowed++;
      } else {
        b.reject++;
        totals.current.rejected++;
      }
      buckets.current.set(sec, b);
      lastKey.current = ev.key;
    };

    const tick = setInterval(() => {
      const now = Math.floor(Date.now() / 1000);
      const series: SecondPoint[] = [];
      for (let t = now - WINDOW + 1; t <= now; t++) {
        const b = buckets.current.get(t);
        series.push({ t, label: hhmmss(t), allow: b?.allow ?? 0, reject: b?.reject ?? 0 });
      }
      // Drop buckets older than the window to bound memory.
      for (const t of buckets.current.keys()) {
        if (t < now - WINDOW) buckets.current.delete(t);
      }
      setState((s) => ({
        ...s,
        series,
        totalAllowed: totals.current.allowed,
        totalRejected: totals.current.rejected,
        lastKey: lastKey.current,
      }));
    }, 1000);

    return () => {
      es.close();
      clearInterval(tick);
    };
  }, []);

  return state;
}

function emptySeries(): SecondPoint[] {
  const now = Math.floor(Date.now() / 1000);
  const out: SecondPoint[] = [];
  for (let t = now - WINDOW + 1; t <= now; t++) out.push({ t, label: hhmmss(t), allow: 0, reject: 0 });
  return out;
}

function hhmmss(unixSec: number): string {
  const d = new Date(unixSec * 1000);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
