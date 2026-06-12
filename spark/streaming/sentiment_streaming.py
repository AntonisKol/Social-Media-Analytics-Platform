# Reads Kafka posts, labels sentiment, groups hashtags into 2-minute windows, and writes live trend results.
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, count, when, regexp_extract_all, explode, lit
from textblob import TextBlob # Simple sentiment analysis library
from shared import HASHTAG_PATTERN, post_schema
from pathlib import Path
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_posts_raw")
CHECKPOINT_DIR = os.getenv("STREAMING_CHECKPOINT_DIR", str(Path(__file__).resolve().parent / "checkpoint"))
OUTPUT_DIR = os.getenv("STREAMING_OUTPUT_DIR", str(Path(__file__).resolve().parent / "output"))
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "2 minutes")

spark = SparkSession.builder \
    .appName("SocialMediaSentimentStreaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def analyse_sentiment(text):
    if not text:
        return "neutral"

    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    if polarity < -0.1:
        return "negative"
    return "neutral"

sentiment_udf = udf(analyse_sentiment)
# Read from Kafka, parse JSON, extract sentiment and hashtags, then aggregate by window, hashtag, and location. Write results to console and Parquet files.
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
# Parse the JSON value from Kafka, extract fields, and keep the Kafka timestamp for windowing.
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
# Enrich the stream with sentiment labels and extract hashtags, then explode the hashtag array for aggregation.
enriched_stream = parsed_stream \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("hashtag", explode(regexp_extract_all(col("text"), lit(HASHTAG_PATTERN), 0)))

# Group by 2-minute windows, hashtag, and location, then count total posts and sentiment breakdowns.
windowed_counts = enriched_stream \
    .withWatermark("kafka_timestamp", "10 minutes") \
    .groupBy(
        window(col("kafka_timestamp"), "2 minutes"),
        col("hashtag"),
        col("location"),
    ) \
    .agg(
        count("*").alias("total_posts"),
        count(when(col("sentiment") == "positive", 1)).alias("positive"),
        count(when(col("sentiment") == "negative", 1)).alias("negative"),
        count(when(col("sentiment") == "neutral", 1)).alias("neutral")
    )

console_query = windowed_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/console") \
    .start()

file_query = windowed_counts \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_DIR) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/file") \
    .start()

spark.streams.awaitAnyTermination()
