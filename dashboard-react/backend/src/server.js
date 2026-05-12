import cors from "cors";
import express from "express";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../../..");

const PORT = Number(process.env.PORT || 4000);
const DATA_DIR = process.env.DASHBOARD_DATA_DIR
  ? path.resolve(process.env.DASHBOARD_DATA_DIR)
  : path.resolve(__dirname, "../data");

const fallbackDataDir = path.resolve(repoRoot, "sample-data");

const app = express();
app.use(cors());

function parseSimpleCsv(text) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length === 0) return [];

  const headers = lines[0].split(",").map((header) => header.trim());
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
  });
}

async function readCsv(primaryName, fallbackName = primaryName) {
  const primaryPath = path.join(DATA_DIR, primaryName);
  const fallbackPath = path.join(fallbackDataDir, fallbackName);

  for (const filePath of [primaryPath, fallbackPath]) {
    try {
      const [text, stat] = await Promise.all([fs.readFile(filePath, "utf8"), fs.stat(filePath)]);
      return {
        rows: parseSimpleCsv(text),
        source: filePath,
        updatedAt: stat.mtime.toISOString(),
        usingFallback: filePath === fallbackPath,
      };
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }

  return { rows: [], source: primaryPath, updatedAt: null, usingFallback: false };
}

function normalizeThroughput(row) {
  const editCount = Number(row.edit_count || 0);
  const botEditCount = Number(row.bot_edit_count || 0);

  return {
    window_start: row.window_start,
    window_end: row.window_end,
    edit_count: editCount,
    bot_edit_count: botEditCount,
    human_edit_count: Math.max(editCount - botEditCount, 0),
    batch_written_at: row.batch_written_at,
  };
}

function normalizeByWiki(row) {
  return {
    window_start: row.window_start,
    window_end: row.window_end,
    wiki: row.wiki,
    edit_count: Number(row.edit_count || 0),
    batch_written_at: row.batch_written_at,
  };
}

function latestByBatch(rows) {
  if (rows.length === 0) return null;
  return rows.reduce((latest, row) =>
    !latest || String(row.batch_written_at) > String(latest.batch_written_at) ? row : latest
  );
}

function summarize(throughputRows, byWikiRows) {
  const latestThroughput = latestByBatch(throughputRows);
  const topWiki = byWikiRows[0] ?? null;
  const latestTotal = latestThroughput?.edit_count ?? 0;
  const latestBots = latestThroughput?.bot_edit_count ?? 0;

  return {
    latest_window_start: latestThroughput?.window_start ?? null,
    latest_window_end: latestThroughput?.window_end ?? null,
    latest_batch_written_at: latestThroughput?.batch_written_at ?? byWikiRows[0]?.batch_written_at ?? null,
    latest_edit_count: latestTotal,
    latest_bot_edit_count: latestBots,
    latest_human_edit_count: Math.max(latestTotal - latestBots, 0),
    latest_bot_percentage: latestTotal ? Number(((latestBots / latestTotal) * 100).toFixed(1)) : 0,
    top_wiki: topWiki?.wiki ?? null,
    top_wiki_edit_count: topWiki?.edit_count ?? 0,
  };
}

async function loadDashboardData() {
  const [throughputCsv, byWikiCsv] = await Promise.all([
    readCsv("throughput_latest.csv", "wiki_pulse_throughput_sample.csv"),
    readCsv("by_wiki_latest.csv", "wiki_pulse_by_wiki_sample.csv"),
  ]);

  const throughput = throughputCsv.rows
    .map(normalizeThroughput)
    .sort((a, b) => String(a.window_start).localeCompare(String(b.window_start)));

  const byWikiRows = byWikiCsv.rows.map(normalizeByWiki);
  const latestByWikiBatch = byWikiRows.reduce(
    (latest, row) => (!latest || String(row.batch_written_at) > String(latest) ? row.batch_written_at : latest),
    null
  );
  const byWiki = byWikiRows
    .filter((row) => !latestByWikiBatch || row.batch_written_at === latestByWikiBatch)
    .sort((a, b) => b.edit_count - a.edit_count);

  return {
    generated_at: new Date().toISOString(),
    data_dir: DATA_DIR,
    sources: {
      throughput: {
        path: throughputCsv.source,
        updated_at: throughputCsv.updatedAt,
        using_fallback: throughputCsv.usingFallback,
      },
      by_wiki: {
        path: byWikiCsv.source,
        updated_at: byWikiCsv.updatedAt,
        using_fallback: byWikiCsv.usingFallback,
      },
    },
    summary: summarize(throughput, byWiki),
    throughput,
    by_wiki: byWiki,
  };
}

app.get("/api/health", async (_req, res, next) => {
  try {
    const data = await loadDashboardData();
    res.json({
      ok: true,
      generated_at: data.generated_at,
      data_dir: data.data_dir,
      sources: data.sources,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/dashboard", async (_req, res, next) => {
  try {
    res.json(await loadDashboardData());
  } catch (error) {
    next(error);
  }
});

app.get("/api/throughput", async (_req, res, next) => {
  try {
    const data = await loadDashboardData();
    res.json({ rows: data.throughput, source: data.sources.throughput });
  } catch (error) {
    next(error);
  }
});

app.get("/api/top-wikis", async (_req, res, next) => {
  try {
    const data = await loadDashboardData();
    res.json({ rows: data.by_wiki, source: data.sources.by_wiki });
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({ ok: false, error: error.message });
});

app.listen(PORT, () => {
  console.log(`Wiki Pulse dashboard API listening on http://localhost:${PORT}`);
  console.log(`CSV data directory: ${DATA_DIR}`);
});
