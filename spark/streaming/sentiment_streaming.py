from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, count, when, regexp_extract_all, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys
from pathlib import Path

SPARK_ROOT = Path(__file__).resolve().parents[1]
if str(SPARK_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_ROOT))

from common.hashtags import HASHTAG_PATTERN
from common.sentiment import analyse_sentiment

# Runtime configuration. Defaults are for host-run spark-submit against Docker Compose.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_posts_raw")
CHECKPOINT_DIR = os.getenv("STREAMING_CHECKPOINT_DIR", str(Path(__file__).resolve().parent / "checkpoint"))
OUTPUT_DIR = os.getenv("STREAMING_OUTPUT_DIR", str(Path(__file__).resolve().parent / "output"))

# create Spark session — Kafka connector package loaded at submit time via --packages
spark = SparkSession.builder \
    .appName("SocialMediaSentimentStreaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# schema matching the JSON format produced by the simulator
# location field added after Phase 2 enrichment was implemented in the simulator
post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("retweets", IntegerType(), True),
    StructField("location", StringType(), True)
])

# register as Spark UDF so it can be applied to DataFrame columns
sentiment_udf = udf(analyse_sentiment, StringType())

# read live stream from Kafka topic social_posts_raw
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka value is raw bytes — cast to string then parse as JSON using the schema
parsed_stream = raw_stream \
    .select(
        from_json(col("value").cast("string"), post_schema).alias("post"),
        col("timestamp").alias("kafka_timestamp")
    ) \
    .select(
        col("post.post_id"),
        col("post.user_id"),
        col("post.text"),
        col("post.likes"),
        col("post.retweets"),
        col("post.location"),
        col("kafka_timestamp")
    )

# enrich each post: classify sentiment and emit one row per hashtag
enriched_stream = parsed_stream \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("hashtag", explode(regexp_extract_all(col("text"), lit(HASHTAG_PATTERN), 0)))

# aggregate into 2-minute tumbling windows grouped by hashtag
# counts total posts and breaks them down by sentiment bucket
windowed_counts = enriched_stream \
    .withWatermark("kafka_timestamp", "10 minutes") \
    .groupBy(
        window(col("kafka_timestamp"), "2 minutes"),
        col("hashtag")
    ) \
    .agg(
        count("*").alias("total_posts"),
        count(when(col("sentiment") == "positive", 1)).alias("positive"),
        count(when(col("sentiment") == "negative", 1)).alias("negative"),
        count(when(col("sentiment") == "neutral", 1)).alias("neutral")
    )

# sink 1: console output for live monitoring during demo
console_query = windowed_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="2 minutes") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/console") \
    .start()

# sink 2: file output as required by task ("write results to output file")
# append mode writes each batch incrementally to parquet files
file_query = windowed_counts \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_DIR) \
    .trigger(processingTime="2 minutes") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/file") \
    .start()

# block until either stream is manually stopped
spark.streams.awaitAnyTermination()
