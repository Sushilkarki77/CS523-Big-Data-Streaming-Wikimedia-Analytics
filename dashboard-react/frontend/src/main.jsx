import React from "react";
import { createRoot } from "react-dom/client";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import "./styles.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:4000";
const REFRESH_MS = Number(import.meta.env.VITE_REFRESH_MS || 15000);

function formatNumber(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function formatPercent(value) {
  return `${Number(value ?? 0).toFixed(1)}%`;
}

function formatTime(value) {
  if (!value) return "n/a";
  const normalized = String(value).replace(" ", "T");
  const date = new Date(normalized.endsWith("Z") ? normalized : `${normalized}Z`);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatWindow(value) {
  if (!value) return "n/a";
  const normalized = String(value).replace(" ", "T");
  const date = new Date(normalized.endsWith("Z") ? normalized : `${normalized}Z`);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function MetricCard({ label, value, hint }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      {hint ? <small>{hint}</small> : null}
    </article>
  );
}

function SourceBadge({ source }) {
  if (!source) return null;
  return (
    <span className={source.using_fallback ? "badge badge-warn" : "badge"}>
      {source.using_fallback ? "sample data" : "Hive CSV"} updated {formatTime(source.updated_at)}
    </span>
  );
}

function Dashboard() {
  const [data, setData] = React.useState(null);
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(true);

  const loadDashboard = React.useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard`);
      if (!response.ok) throw new Error(`API returned ${response.status}`);
      const nextData = await response.json();
      setData(nextData);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    loadDashboard();
    const id = window.setInterval(loadDashboard, REFRESH_MS);
    return () => window.clearInterval(id);
  }, [loadDashboard]);

  const throughput = data?.throughput ?? [];
  const byWiki = data?.by_wiki ?? [];
  const summary = data?.summary ?? {};

  const chartThroughput = throughput.map((row) => ({
    ...row,
    label: formatWindow(row.window_start),
  }));

  return (
    <main>
      <header className="hero">
        <div>
          <p className="eyebrow">Big Data Technologies Final Project</p>
          <h1>Wiki Pulse Dashboard</h1>
          <p className="subtitle">
            Live Wikimedia recent-change metrics exported from Hive snapshots.
          </p>
        </div>
        <button type="button" onClick={loadDashboard} disabled={loading}>
          {loading ? "Loading..." : "Refresh now"}
        </button>
      </header>

      {error ? <section className="alert">API error: {error}</section> : null}

      <section className="status-row">
        <SourceBadge source={data?.sources?.throughput} />
        <SourceBadge source={data?.sources?.by_wiki} />
        <span className="badge">polls every {Math.round(REFRESH_MS / 1000)}s</span>
      </section>

      <section className="metrics-grid">
        <MetricCard
          label="Latest window events"
          value={formatNumber(summary.latest_edit_count)}
          hint={`${formatWindow(summary.latest_window_start)} - ${formatWindow(summary.latest_window_end)}`}
        />
        <MetricCard
          label="Bot events"
          value={formatNumber(summary.latest_bot_edit_count)}
          hint={`${formatPercent(summary.latest_bot_percentage)} of latest window`}
        />
        <MetricCard
          label="Human events"
          value={formatNumber(summary.latest_human_edit_count)}
          hint="derived from total - bot"
        />
        <MetricCard
          label="Top project"
          value={summary.top_wiki || "n/a"}
          hint={`${formatNumber(summary.top_wiki_edit_count)} events`}
        />
      </section>

      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Throughput Over Time</h2>
            <p>Total and bot-flagged Wikimedia events by event-time window.</p>
          </div>
        </div>
        <div className="chart">
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={chartThroughput}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="label" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="edit_count" name="Total events" stroke="#4f46e5" strokeWidth={3} dot={false} />
              <Line type="monotone" dataKey="bot_edit_count" name="Bot events" stroke="#f97316" strokeWidth={3} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Top Wikimedia Projects</h2>
            <p>Highest event counts in the latest exported Hive snapshot.</p>
          </div>
        </div>
        <div className="chart">
          <ResponsiveContainer width="100%" height={360}>
            <BarChart data={byWiki.slice(0, 15)} layout="vertical" margin={{ left: 48 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="wiki" type="category" width={170} />
              <Tooltip />
              <Bar dataKey="edit_count" name="Events" fill="#10b981" radius={[0, 8, 8, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="table-panel">
        <h2>Latest Top Wiki Rows</h2>
        <table>
          <thead>
            <tr>
              <th>Wiki</th>
              <th>Events</th>
              <th>Window</th>
              <th>Batch written</th>
            </tr>
          </thead>
          <tbody>
            {byWiki.slice(0, 10).map((row) => (
              <tr key={`${row.wiki}-${row.batch_written_at}`}>
                <td>{row.wiki}</td>
                <td>{formatNumber(row.edit_count)}</td>
                <td>{formatWindow(row.window_start)}</td>
                <td>{formatTime(row.batch_written_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<Dashboard />);
