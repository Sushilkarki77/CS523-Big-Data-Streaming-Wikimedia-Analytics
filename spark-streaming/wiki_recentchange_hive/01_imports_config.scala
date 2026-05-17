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
