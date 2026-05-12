import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SHUFFLE_PARTITIONS", "4"))
spark.sparkContext.setLogLevel(sys.env.getOrElse("SPARK_LOG_LEVEL", "WARN"))

val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-server:9092")
val topic = sys.env.getOrElse("KAFKA_TOPIC_RAW", "bdt-wikimedia-recentchange")
val startingOffsets = sys.env.getOrElse("SPARK_STARTING_OFFSETS", "latest")
val checkpointRoot = sys.env.getOrElse(
  "SPARK_CHECKPOINT_DIR",
  "hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive"
)
val windowDuration = sys.env.getOrElse("SPARK_WINDOW_DURATION", "5 minutes")
val watermarkDelay = sys.env.getOrElse("SPARK_WATERMARK_DELAY", "10 minutes")
val triggerInterval = sys.env.getOrElse("SPARK_TRIGGER_INTERVAL", "30 seconds")
val hiveDatabase = sys.env.getOrElse("HIVE_DATABASE", "wiki_pulse")
val throughputTable = s"$hiveDatabase.wiki_pulse_throughput"
val byWikiTable = s"$hiveDatabase.wiki_pulse_by_wiki"
val byProjectFamilyTable = s"$hiveDatabase.wiki_pulse_by_project_family"
val throughputPath = sys.env.getOrElse(
  "HIVE_THROUGHPUT_PATH",
  "hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_throughput"
)
val byWikiPath = sys.env.getOrElse(
  "HIVE_BY_WIKI_PATH",
  "hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_wiki"
)
val byProjectFamilyPath = sys.env.getOrElse(
  "HIVE_BY_PROJECT_FAMILY_PATH",
  "hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_project_family"
)
val staticWikiLookupPath = sys.env.getOrElse(
  "STATIC_WIKI_LOOKUP_PATH",
  "hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv"
)
val shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions")

println(
  s"""
     |Starting Wikimedia recentchange Spark -> Hive job
     |  bootstrapServers = $bootstrapServers
     |  topic            = $topic
     |  startingOffsets  = $startingOffsets
     |  checkpointRoot   = $checkpointRoot
     |  windowDuration   = $windowDuration
     |  watermarkDelay   = $watermarkDelay
     |  triggerInterval  = $triggerInterval
     |  shufflePartitions= $shufflePartitions
     |  throughputTable  = $throughputTable
     |  byWikiTable      = $byWikiTable
     |  familyTable      = $byProjectFamilyTable
     |  throughputPath   = $throughputPath
     |  byWikiPath       = $byWikiPath
     |  familyPath       = $byProjectFamilyPath
     |  staticLookupPath = $staticWikiLookupPath
     |""".stripMargin
)

val wikiLookup = spark.read
  .option("header", "true")
  .csv(staticWikiLookupPath)
  .select(
    col("wiki"),
    col("project_family"),
    col("language"),
    col("region")
  )
  .dropDuplicates("wiki")

val eventSchema = new StructType()
  .add("event_time", StringType, nullable = false)
  .add("ingest_time", StringType, nullable = false)
  .add("source", StringType, nullable = false)
  .add("schema_version", StringType, nullable = false)
  .add("wiki", StringType, nullable = false)
  .add("title", StringType, nullable = true)
  .add("namespace_id", IntegerType, nullable = true)
  .add("event_type", StringType, nullable = true)
  .add("user", StringType, nullable = true)
  .add("bot", BooleanType, nullable = true)
  .add("minor", BooleanType, nullable = true)
  .add("comment", StringType, nullable = true)
  .add("meta_uri", StringType, nullable = true)

val kafkaRecords = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrapServers)
  .option("subscribe", topic)
  .option("startingOffsets", startingOffsets)
  .option("failOnDataLoss", "false")
  .load()

val parsedEvents = kafkaRecords
  .selectExpr("CAST(value AS STRING) AS json")
  .select(from_json(col("json"), eventSchema).as("event"))
  .select("event.*")
  .filter(col("schema_version") === lit("1.0"))
  .withColumn(
    "event_ts",
    coalesce(
      to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
      to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
  )
  .filter(col("event_ts").isNotNull)
  .filter(col("wiki").isNotNull && length(col("wiki")) > 0)

val watermarkedEvents = parsedEvents.withWatermark("event_ts", watermarkDelay)

val enrichedEvents = watermarkedEvents
  .join(broadcast(wikiLookup), Seq("wiki"), "left")
  .withColumn("project_family", coalesce(col("project_family"), lit("Other")))
  .withColumn("language", coalesce(col("language"), lit("unknown")))
  .withColumn("region", coalesce(col("region"), lit("unknown")))

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

def writeHiveSnapshot(tableName: String, outputPath: String, sortColumns: Seq[org.apache.spark.sql.Column])(
    batchDF: DataFrame,
    batchId: Long
): Unit = {
  val rows = batchDF.count()
  if (rows == 0) {
    println(s"Batch $batchId -> $tableName: no rows")
    return
  }

  val output = batchDF
    .withColumn("batch_written_at", current_timestamp())
    .repartition(1)

  // Write directly to the Hive table's HDFS location so Hive CLI can read it without HiveServer2.
  output.write.mode("append").parquet(outputPath)

  println(s"Batch $batchId -> $tableName: appended $rows rows at $outputPath")
  val sorted = if (sortColumns.nonEmpty) output.orderBy(sortColumns: _*) else output
  sorted.show(10, truncate = false)
}

val throughputQuery = throughput.writeStream
  .queryName("wiki_pulse_throughput_hive")
  .outputMode("update")
  .option("checkpointLocation", s"$checkpointRoot/throughput")
  .trigger(Trigger.ProcessingTime(triggerInterval))
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    writeHiveSnapshot(throughputTable, throughputPath, Seq(col("window_start").desc))(batchDF, batchId)
  }
  .start()

val byWikiQuery = byWiki.writeStream
  .queryName("wiki_pulse_by_wiki_hive")
  .outputMode("update")
  .option("checkpointLocation", s"$checkpointRoot/by-wiki")
  .trigger(Trigger.ProcessingTime(triggerInterval))
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    writeHiveSnapshot(byWikiTable, byWikiPath, Seq(col("window_start").desc, col("edit_count").desc))(batchDF, batchId)
  }
  .start()

val byProjectFamilyQuery = byProjectFamily.writeStream
  .queryName("wiki_pulse_by_project_family_hive")
  .outputMode("update")
  .option("checkpointLocation", s"$checkpointRoot/by-project-family")
  .trigger(Trigger.ProcessingTime(triggerInterval))
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    writeHiveSnapshot(
      byProjectFamilyTable,
      byProjectFamilyPath,
      Seq(col("window_start").desc, col("edit_count").desc)
    )(batchDF, batchId)
  }
  .start()

println("Hive streaming queries started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
