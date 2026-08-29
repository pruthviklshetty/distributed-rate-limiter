// Types and fetch helpers for the rate-limiter API. Shapes mirror the Go
// structs in internal/api and internal/stats.

export interface TierInfo {
  name: string;
  limit: number;
  refillPerSec?: number;
  windowSeconds?: number;
}

export interface ConfigInfo {
  algorithm: string;
  keyBy: string;
  backend: string;
  tiers: TierInfo[];
}

export interface KeyStat {
  key: string;
  allowed: number;
  rejected: number;
  lastRemaining: number;
  lastSeenUnix: number;
}

export interface PerSecond {
  second: number;
  allow: number;
  reject: number;
}

export interface Snapshot {
  totalAllowed: number;
  totalRejected: number;
  dropped: number;
  trackedKeys: number;
  perSecond: PerSecond[];
  topKeys: KeyStat[];
}

// One decision as streamed over SSE (internal/api eventDTO).
export interface LiveEvent {
  key: string;
  algorithm: string;
  allowed: boolean;
  remaining: number;
  tsUnixMs: number;
  latencyUs: number;
}

export async function getConfig(): Promise<ConfigInfo> {
  const r = await fetch("/api/config");
  if (!r.ok) throw new Error(`/api/config ${r.status}`);
  return r.json();
}

export async function getStats(): Promise<Snapshot> {
  const r = await fetch("/api/stats?seconds=60&top=8");
  if (!r.ok) throw new Error(`/api/stats ${r.status}`);
  return r.json();
}

export async function sendBurst(key: string, count: number): Promise<{ allowed: number; rejected: number }> {
  const r = await fetch("/api/demo/burst", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, count }),
  });
  if (!r.ok) throw new Error(`/api/demo/burst ${r.status}`);
  return r.json();
}

// setAlgorithm switches the live limiter's algorithm and returns the updated
// config. Valid values: "token-bucket", "sliding-window".
export async function setAlgorithm(algorithm: string): Promise<ConfigInfo> {
  const r = await fetch("/api/config/algorithm", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ algorithm }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.error || `/api/config/algorithm ${r.status}`);
  }
  return r.json();
}
