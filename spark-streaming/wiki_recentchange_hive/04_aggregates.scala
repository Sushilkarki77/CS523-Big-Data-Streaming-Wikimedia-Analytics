val throughput = enrichedEvents
  .groupBy(window(col("event_ts"), windowDuration))
  .agg(
    count(lit(1)).as("edit_count"),
    sum(when(coalesce(col("bot"), lit(false)), lit(1L)).otherwise(lit(0L))).as("bot_edit_count")
  )
  .select(
    col("window.start").as("window_start"),
    col("window.end").as("window_end"),
    col("edit_count"),
    col("bot_edit_count")
  )

val byWiki = enrichedEvents
  .groupBy(window(col("event_ts"), windowDuration), col("wiki"))
  .agg(count(lit(1)).as("edit_count"))
  .select(
    col("window.start").as("window_start"),
    col("window.end").as("window_end"),
    col("wiki"),
    col("edit_count")
  )

val byProjectFamily = enrichedEvents
  .groupBy(window(col("event_ts"), windowDuration), col("project_family"))
  .agg(count(lit(1)).as("edit_count"))
  .select(
    col("window.start").as("window_start"),
    col("window.end").as("window_end"),
    col("project_family"),
    col("edit_count")
  )
