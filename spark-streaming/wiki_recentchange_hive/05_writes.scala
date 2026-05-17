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
