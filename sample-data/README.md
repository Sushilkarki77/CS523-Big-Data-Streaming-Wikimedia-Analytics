# Sample aggregate data (for Sudipto — mock Hive output)

These CSVs match the column layout in **`docs/sink-spec.md`** so you can build charts **before** Sushil’s Spark job writes real Hive tables.

| File | Hive table name (suggested) | Use for |
|------|------------------------------|---------|
| **`wiki_pulse_throughput_sample.csv`** | `wiki_pulse_throughput` | Line chart: `window_start` vs `edit_count`; bot split: `bot_edit_count` vs (`edit_count` − `bot_edit_count`) |
| **`wiki_pulse_by_wiki_sample.csv`** | `wiki_pulse_by_wiki` | Bar chart: top wikis — filter `batch_written_at = max(batch_written_at)` for latest snapshot |
| **`wiki_pulse_by_project_family_sample.csv`** | `wiki_pulse_by_project_family` | Bonus bar chart: project families after static HDFS CSV enrichment |

## Column meanings

### Throughput

- **`window_start` / `window_end`**: 5-minute tumbling window (UTC, ISO-8601).
- **`edit_count`**: total edits in that window (all wikis).
- **`bot_edit_count`**: subset flagged bot in source stream.
- **`batch_written_at`**: when the streaming batch landed (processing time).

### Per wiki

- **`window_start` / `window_end`**: must align with throughput windows.
- **`wiki`**: domain string (same style as Kafka `wiki` field).
- **`edit_count`**: edits for that wiki in the window.
- **`batch_written_at`**: when the streaming batch landed (processing time).

### Per project family (bonus)

- **`window_start` / `window_end`**: must align with throughput windows.
- **`project_family`**: joined from the static HDFS CSV lookup.
- **`edit_count`**: edits for that family in the window.
- **`batch_written_at`**: when the streaming batch landed (processing time).

## Quick load (Python / Streamlit)

```python
import pandas as pd
from pathlib import Path

root = Path(__file__).resolve().parent.parent / "sample-data"
throughput = pd.read_csv(root / "wiki_pulse_throughput_sample.csv", parse_dates=["window_start", "window_end", "batch_written_at"])
by_wiki = pd.read_csv(root / "wiki_pulse_by_wiki_sample.csv", parse_dates=["window_start", "window_end", "batch_written_at"])
by_family = pd.read_csv(root / "wiki_pulse_by_project_family_sample.csv", parse_dates=["window_start", "window_end", "batch_written_at"])

latest_batch = by_wiki["batch_written_at"].max()
top_wikis = by_wiki[by_wiki["batch_written_at"] == latest_batch].sort_values("edit_count", ascending=False)

latest_family_batch = by_family["batch_written_at"].max()
top_families = by_family[by_family["batch_written_at"] == latest_family_batch].sort_values("edit_count", ascending=False)
```

## After Hive is live

Point the same dashboard code at **HiveServer2 / JDBC** or **Spark SQL** using the same column names; swap only the data source.
