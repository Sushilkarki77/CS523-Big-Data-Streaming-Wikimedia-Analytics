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
