import { useCallback, useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  getConfig,
  getStats,
  sendBurst,
  setAlgorithm,
  type ConfigInfo,
  type Snapshot,
} from "./api";
import { useLiveSeries } from "./useLiveSeries";

const ALGORITHMS = [
  { value: "token-bucket", label: "Token bucket" },
  { value: "sliding-window", label: "Sliding window" },
];

export function App() {
  const live = useLiveSeries();
  const [config, setConfig] = useState<ConfigInfo | null>(null);
  const [stats, setStats] = useState<Snapshot | null>(null);
  const [burstKey, setBurstKey] = useState("demo");
  const [burstCount, setBurstCount] = useState(50);
  const [busy, setBusy] = useState(false);
  const [lastBurst, setLastBurst] = useState<string | null>(null);
  const [switchingAlgo, setSwitchingAlgo] = useState(false);
  const [algoError, setAlgoError] = useState<string | null>(null);

  useEffect(() => {
    getConfig().then(setConfig).catch(() => setConfig(null));
  }, []);

  useEffect(() => {
    const load = () => getStats().then(setStats).catch(() => {});
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  const changeAlgo = useCallback(async (next: string) => {
    setSwitchingAlgo(true);
    setAlgoError(null);
    try {
      setConfig(await setAlgorithm(next));
    } catch (e) {
      setAlgoError(String(e instanceof Error ? e.message : e));
    } finally {
      setSwitchingAlgo(false);
    }
  }, []);

  const fireBurst = useCallback(async () => {
    setBusy(true);
    setLastBurst(null);
    try {
      const r = await sendBurst(burstKey || "demo", burstCount);
      setLastBurst(`${r.allowed} allowed · ${r.rejected} rejected (429)`);
    } catch (e) {
      setLastBurst(`error: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }, [burstKey, burstCount]);

  return (
    <div className="wrap">
      <header>
        <h1>API Rate Limiter — live</h1>
        <p className="sub">
          Every request below is checked against a rate limiter. Hit{" "}
          <strong>Send burst</strong> and watch rejections (HTTP&nbsp;429) appear in real time.
        </p>
      </header>

      <section className="panel config">
        {config ? (
          <>
            <div className="statbox">
              <span className="statlabel">Algorithm</span>
              <select
                className="algo-select"
                value={config.tiers[0]?.name ?? "token-bucket"}
                disabled={switchingAlgo}
                onChange={(e) => changeAlgo(e.target.value)}
              >
                {ALGORITHMS.map((a) => (
                  <option key={a.value} value={a.value}>
                    {a.label}
                  </option>
                ))}
              </select>
            </div>
            <Stat label="Backend" value={config.backend} />
            <Stat label="Key by" value={config.keyBy} />
            {config.tiers.map((t) => (
              <Stat
                key={t.name}
                label={`Limit (${t.name})`}
                value={
                  t.windowSeconds
                    ? `${t.limit} / ${t.windowSeconds}s`
                    : `${t.limit} burst${t.refillPerSec ? `, +${t.refillPerSec}/s` : ""}`
                }
              />
            ))}
          </>
        ) : (
          <span className="muted">loading config…</span>
        )}
        <span className={`conn ${live.connected ? "ok" : "down"}`}>
          {live.connected ? "● live stream connected" : "○ live stream offline"}
        </span>
      </section>
      {algoError && <div className="algo-error">algorithm switch failed: {algoError}</div>}

      <section className="panel burst">
        <div className="burst-controls">
          <label>
            key
            <input value={burstKey} onChange={(e) => setBurstKey(e.target.value)} spellCheck={false} />
          </label>
          <label>
            requests
            <input
              type="number"
              min={1}
              max={1000}
              value={burstCount}
              onChange={(e) => setBurstCount(Math.max(1, Math.min(1000, Number(e.target.value) || 1)))}
            />
          </label>
          <button onClick={fireBurst} disabled={busy}>
            {busy ? "sending…" : `Send burst of ${burstCount}`}
          </button>
        </div>
        {lastBurst && <div className="burst-result">{lastBurst}</div>}
      </section>

      <section className="panel chart">
        <div className="chart-head">
          <h2>Allowed vs rejected — per second</h2>
          <div className="counters">
            <span className="c-allow">{live.totalAllowed.toLocaleString()} allowed</span>
            <span className="c-reject">{live.totalRejected.toLocaleString()} rejected</span>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={live.series} margin={{ top: 8, right: 12, bottom: 0, left: -18 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#26304a" />
            <XAxis dataKey="label" tick={{ fill: "#8a97b1", fontSize: 11 }} interval={9} />
            <YAxis allowDecimals={false} tick={{ fill: "#8a97b1", fontSize: 11 }} />
            <Tooltip
              contentStyle={{ background: "#111726", border: "1px solid #26304a", borderRadius: 8 }}
              labelStyle={{ color: "#8a97b1" }}
            />
            <Area
              type="monotone"
              dataKey="allow"
              name="allowed"
              stackId="1"
              stroke="#3ddc91"
              fill="#3ddc91"
              fillOpacity={0.25}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="reject"
              name="rejected"
              stackId="1"
              stroke="#ff5c72"
              fill="#ff5c72"
              fillOpacity={0.3}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </section>

      <section className="panel table">
        <h2>Top keys</h2>
        <table>
          <thead>
            <tr>
              <th>key</th>
              <th className="num">remaining</th>
              <th className="num">allowed</th>
              <th className="num">rejected</th>
            </tr>
          </thead>
          <tbody>
            {stats && stats.topKeys.length > 0 ? (
              stats.topKeys.map((k) => (
                <tr key={k.key}>
                  <td className="mono">{k.key}</td>
                  <td className="num">{k.lastRemaining}</td>
                  <td className="num">{k.allowed.toLocaleString()}</td>
                  <td className={`num ${k.rejected > 0 ? "bad" : ""}`}>{k.rejected.toLocaleString()}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={4} className="muted">
                  no traffic yet — send a burst
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <footer>
        Portfolio project · token-bucket &amp; sliding-window algorithms · in-memory / Redis backends ·
        SSE live stream
      </footer>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="statbox">
      <span className="statlabel">{label}</span>
      <span className="statvalue">{value}</span>
    </div>
  );
}
