# Demo-readiness cleanup plan

**Purpose:** Make the repo easy to demo in class or for grading — one clear path, minimal duplication, docs that match what actually runs.

**Status:** **Completed** (May 2026). Console job kept under `spark-streaming/dev/`; planning docs under `docs/archive/`; demo path in `DEMO.md` + slim `README.md`.

---

## 1. Demo-critical path (keep)

These are the only pieces needed for the **full live demo**:

| Step | Command / artifact |
|------|-------------------|
| Docker stack | `kafka-server`, `zookeeper-server`, `cs523bdt-lab`, `hive-metastore-db` |
| Optional reset | `bash scripts/repair-demo-hdfs-state.sh --reset` |
| Topic | `bash scripts/create-project-topic.sh` |
| Static lookup | `bash scripts/upload-static-wiki-lookup.sh` |
| Hive DDL | `bash scripts/create-hive-tables.sh` |
| Producer | `bash scripts/run-producer-docker.sh` |
| Spark → Hive | `bash scripts/run-spark-streaming-hive.sh` |
| Export | `bash scripts/export-hive-dashboard-data.sh` (loop every 120s) |
| API + UI | `dashboard-react/backend` + `frontend` (`npm run dev`) |
| Stop | `bash scripts/stop-everything.sh` |

**One-command alternative:** `bash scripts/start-demo.sh` + `bash scripts/stop-demo.sh` (does not stop Docker lab unless you use `stop-everything.sh --containers`).

---

## 2. Recommended target structure (after cleanup)

```text
final-project/
├── README.md                    # Short: prerequisites, demo start/stop, troubleshooting links
├── DEMO.md                      # NEW: single-page demo script (5 min + 20 min manual)
├── .env.example
├── producer/
│   └── wikimedia_kafka_producer.py
├── spark-streaming/
│   ├── wiki_recentchange_hive/  # production demo job (keep)
│   └── wiki_recentchange_console.scala  # optional: dev only or archive
├── scripts/                     # see §4
├── sql/hive/
├── static-data/
├── dashboard-react/
├── docs/
│   ├── architecture.md          # keep (diagram)
│   ├── kafka-message-contract.md
│   ├── sink-spec.md
│   └── archive/                 # move planning/phase docs here OR delete
└── sample-data/                 # keep only if dashboard fallback required
```

---

## 3. Code — remove or demote

### 3.1 Safe to remove for demo-only repo (if rubric allows)

| Item | Lines (approx) | Reason |
|------|----------------|--------|
| `scripts/phase0-verify.sh` | ~44 | Course stack smoke test; uses `phase0-healthcheck` topic, not project topic |
| `docs/phase0-inventory.md` | 108 | Superseded by README prerequisites |
| `docs/team-parallel-plan.md` | 102 | Internal planning, not needed at demo |
| `docs/two-developer-plan-sushil-viz.md` | 185 | Same |
| `docs/implementation-plan.md` | 258 | Historical phases; merge “done” summary into README or DEMO.md |
| `scripts/send-sample-contract-message.sh` | small | Redundant if `verify-producer.sh` + live producer exist |
| Duplicate path noise | — | Windows duplicate `scripts\export-hive-dashboard-data.sh` in git index only if both exist; ensure single file |

### 3.2 Keep but mark “dev / optional”

| Item | Reason to keep |
|------|----------------|
| `spark-streaming/wiki_recentchange_console.scala` | Proves Phase 3 without Hive; good for debugging Kafka parse/windows |
| `scripts/run-spark-streaming-console.sh` | Launcher for console job |
| `scripts/verify-producer.sh` | Fast smoke test without starting long-running producer |
| `sample-data/*.csv` | Dashboard works offline when Hive export fails (`server.js` fallback) |
| Local Python producer path in README | Optional; Docker producer is enough for demo |

### 3.3 Do not remove (recently added, demo-critical)

| Item | Reason |
|------|--------|
| `scripts/repair-demo-hdfs-state.sh` | Fixes safe mode / corrupt HDFS after Docker restarts |
| `scripts/stop-everything.sh` | Clean shutdown for manual multi-terminal demo |
| `spark-streaming/wiki_recentchange_hive/` fragments | Maintainability; assembled by `run-spark-streaming-hive.sh` |

---

## 4. Scripts — consolidate

### 4.1 Essential (10)

```text
start-demo.sh
stop-demo.sh
stop-everything.sh
create-project-topic.sh
upload-static-wiki-lookup.sh
create-hive-tables.sh
repair-demo-hdfs-state.sh
run-producer-docker.sh
run-spark-streaming-hive.sh
export-hive-dashboard-data.sh
```

### 4.2 Optional dev (3)

```text
verify-producer.sh
run-spark-streaming-console.sh
send-sample-contract-message.sh
```

### 4.3 Suggested merges (next session)

| Idea | Benefit |
|------|---------|
| **`scripts/demo-setup.sh`** | Single script: `repair --auto` + topic + upload lookup + create tables (what `start-demo` does before background jobs) |
| **`scripts/demo-export-loop.sh`** | Wraps the `while true; export; sleep` loop (interval from env) — avoids users pasting broken loops |
| **Fold `stop-demo` into `stop-everything`** | One stop script with flags: `--pids-only` (default for start-demo) vs `--all` (current stop-everything) |

Avoid growing script count; prefer **flags on fewer scripts**.

---

## 5. Spark — deduplicate console vs Hive

**Problem:** `wiki_recentchange_console.scala` (~130 lines) and `wiki_recentchange_hive/*` (~206 lines combined) duplicate:

- JSON `eventSchema`
- Kafka `readStream` + `parsedEvents` + watermark
- `throughput` and `byWiki` aggregates

**Options (pick one next session):**

| Option | Effort | Demo impact |
|--------|--------|-------------|
| **A. Delete console job** | Low | Demo uses Hive path only; document “Phase 3 validated via Hive job logs” |
| **B. Shared fragments** | Medium | Add `spark-streaming/common/01_kafka_parse.scala` included by both assembly scripts |
| **C. Keep console, move to `spark-streaming/dev/`** | Low | Clear “not used in demo” |

**Recommendation for demo-ready repo:** **C** or **A**. **B** only if you want both jobs long-term.

**Hive-only extras (do not merge into console):**

- `02_lookup.scala` — static CSV join
- `byProjectFamily` aggregate
- `writeHiveSnapshot` + three `foreachBatch` Parquet writes

---

## 6. Producer — trim?

`producer/wikimedia_kafka_producer.py` (~242 lines) is reasonable. Optional trims:

| Trim | Risk |
|------|------|
| Remove unused CLI flags | Low — grep `argparse` usage first |
| Shorten reconnect/backoff comments | None |
| Drop local-run docs from README if Docker-only demo | Doc only |

**Do not** remove contract mapping (`map_to_contract`) or SSE parsing — that is the rubric story.

---

## 7. Dashboard — trim?

| File | Notes |
|------|--------|
| `dashboard-react/backend/src/server.js` (~229 lines) | Keep; fallback to `sample-data/` is valuable for dry runs |
| `dashboard-react/frontend/src/main.jsx` (~237 lines) | Single-file UI is fine for demo; split into components only if you want polish |
| `dashboard-react/frontend/src/styles.css` | Keep |

**Doc fix:** `dashboard-react/README.md` still says `sleep 30` for export loop; align with **120s** (matches `start-demo.sh` default and realistic Hive export time).

**Optional:** Add `scripts/demo-export-loop.sh` and reference it from README instead of inline `while`.

---

## 8. Documentation — consolidate (high impact)

**Problem:** ~2,000+ lines across 12 `docs/*.md` files plus 310-line `README.md`. Much content repeats phases, commands, and troubleshooting.

### 8.1 Proposed doc tiers

| Tier | Files | Audience |
|------|-------|----------|
| **Demo** | `DEMO.md` (new), slim `README.md` | You + grader in 5 minutes |
| **Reference** | `docs/architecture.md`, `docs/kafka-message-contract.md`, `docs/sink-spec.md` | Design + schema |
| **Deep dive** | `docs/run-spark-streaming-hive.md`, `docs/producer-wikimedia-kafka.md` | Optional reading |
| **Archive** | `implementation-plan`, `team-*`, `phase0`, `two-developer-*`, `current-end-to-end-flow` | Move to `docs/archive/` or delete |

### 8.2 `DEMO.md` outline (create next session)

1. Prerequisites (4 containers)
2. Fresh start: `repair --reset` → setup scripts
3. **Manual demo** (5 terminals) — copy-paste commands
4. **One-command demo** — `start-demo.sh`
5. URLs + what to show on screen
6. Hive query examples (3 tables)
7. Troubleshooting (safe mode, BlockMissing, empty dashboard, export 1–2 min silence)
8. Stop commands

### 8.3 Slim `README.md` target (~80–120 lines)

- Badges / one-line description
- Prerequisites
- Quick start: `start-demo.sh`
- Link to `DEMO.md` for manual steps
- Link to architecture + contract
- Short troubleshooting + repair command

Move Phase 1–5 long sections out of README into `DEMO.md` or archive.

### 8.4 Overlap to merge

| Duplicate topic | Currently in | Keep in |
|-----------------|--------------|---------|
| End-to-end flow diagram | `architecture.md`, `current-end-to-end-flow.md`, README | `architecture.md` only |
| Export loop command | README, dashboard-react/README, `export-hive-dashboard-data.sh` header | `DEMO.md` + script header |
| Spark run instructions | README Phase 3–4, `spark-streaming/README`, `run-spark-streaming-hive.md` | `DEMO.md` + `run-spark-streaming-hive.md` |
| Producer Docker vs local | README, `producer-wikimedia-kafka.md` | `producer-wikimedia-kafka.md` (detail), README one paragraph |

---

## 9. Configuration and env

| Item | Action |
|------|--------|
| `.env.example` | Ensure all vars used by Spark script and producer are documented once |
| Remove unused env vars | Grep `sys.env` and `os.environ` vs `.env.example` |
| Demo defaults | Consider `SPARK_WINDOW_DURATION=1 minute` and `SPARK_TRIGGER_INTERVAL=15 seconds` in `.env.example` as **commented** demo profile |

---

## 10. Git / repo hygiene

| Item | Action |
|------|--------|
| `.gitignore` | Confirm `.demo/`, `node_modules/`, `dashboard-react/backend/data/*.csv` (if generated — or commit sample CSVs only in `sample-data/`) |
| `dashboard-react/backend/data/*.csv` | **Decision:** gitignore generated CSVs; rely on export or `sample-data` fallback |
| `package-lock.json` | Keep (reproducible npm install) |
| PDF in repo | `FinalProject (1).pdf` — move outside repo or `docs/` if not needed for runtime |

---

## 11. Prioritized checklist (next session)

### P0 — Demo works reliably (1–2 hours)

- [x] Create `DEMO.md` with manual + one-command flows
- [x] Slim `README.md` (link to DEMO.md)
- [x] Add `scripts/demo-export-loop.sh` (optional but reduces user error)
- [x] Align export sleep: 120s in `dashboard-react/README.md` and `start-demo.sh`
- [x] Confirm `.gitignore` for `backend/data/*.csv` (except `.gitkeep`)

### P1 — Less clutter (2–3 hours)

- [x] Move `docs/implementation-plan.md`, `team-parallel-plan.md`, `two-developer-plan-sushil-viz.md`, `phase0-inventory.md` → `docs/archive/`
- [x] Merge or delete `docs/current-end-to-end-flow.md` (content in `architecture.md` + `DEMO.md`)
- [x] Move `scripts/phase0-verify.sh` → `scripts/dev/`
- [x] Console Spark job: **kept in `spark-streaming/dev/`**

### P2 — Code quality (3+ hours)

- [ ] Extract shared Spark Kafka parse fragments (`common/`) — deferred
- [x] Consolidate stop scripts (`stop-demo` + `stop-everything` flags)
- [x] Add `scripts/demo-setup.sh` used by `start-demo.sh`
- [ ] Trim producer only if argparse proves dead code

### P3 — Polish (optional)

- [ ] Split `main.jsx` into small components
- [ ] Add `npm run demo` at repo root via root `package.json` that echoes instructions
- [ ] Pre-flight script: `scripts/preflight-demo.sh` (containers + safe mode + topic exists)

---

## 12. Pre-demo checklist (day of presentation)

```bash
docker ps   # 4 containers
bash scripts/repair-demo-hdfs-state.sh --auto
bash scripts/create-project-topic.sh
bash scripts/upload-static-wiki-lookup.sh
bash scripts/create-hive-tables.sh

# Terminal 1
bash scripts/run-producer-docker.sh

# Terminal 2
bash scripts/run-spark-streaming-hive.sh

# Terminal 3
bash scripts/export-hive-dashboard-data.sh   # once, wait for success
# then loop or demo-export-loop.sh

# Terminals 4–5: backend + frontend npm run dev

# Verify
curl -s http://localhost:4000/api/health
open http://localhost:5173
```

---

## 13. What not to change

- Hive “write Parquet to warehouse path” pattern (no HiveServer2) — core design
- Kafka message contract and topic name — grading alignment
- Three Hive summary tables + project_family bonus
- CSV export bridge (simple, works in course environment)
- `repair-demo-hdfs-state.sh` — saves hours of debugging on Windows Docker

---

## 14. Open decisions (resolve before large deletes)

1. **Is Phase 3 console Spark job required in the report?** → drives keep/delete of `wiki_recentchange_console.scala`
2. **Are team/planning docs required in submission?** → drives archive vs delete
3. **Must grader run without internet?** → Wikimedia SSE + Maven `--packages` need network on first Spark start
4. **Commit generated CSVs or not?** → affects dashboard out-of-box experience vs clean git

---

*Generated for cleanup session. Update this file as items are completed.*
