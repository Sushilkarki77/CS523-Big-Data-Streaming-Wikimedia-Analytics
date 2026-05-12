CREATE DATABASE IF NOT EXISTS wiki_pulse;

USE wiki_pulse;

CREATE TABLE IF NOT EXISTS wiki_pulse_throughput (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  edit_count BIGINT,
  bot_edit_count BIGINT,
  batch_written_at TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wiki_pulse_by_wiki (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  wiki STRING,
  edit_count BIGINT,
  batch_written_at TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wiki_pulse_by_project_family (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  project_family STRING,
  edit_count BIGINT,
  batch_written_at TIMESTAMP
)
STORED AS PARQUET;
