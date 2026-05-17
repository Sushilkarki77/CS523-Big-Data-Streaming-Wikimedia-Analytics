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
