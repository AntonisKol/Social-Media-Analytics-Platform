from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, count, when, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from textblob import TextBlob

# create the Spark session with Kafka connector package
spark = SparkSession.builder \
    .appName("SocialMediaSentimentStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.kafka.consumer.cache.enabled", "false") \
    .getOrCreate()

# set log level to ERROR to reduce noise in the console output
spark.sparkContext.setLogLevel("ERROR")

# define the schema of the JSON posts coming from Kafka
post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("retweets", IntegerType(), True)
])

# define a Python function that takes post text and returns sentiment label
def analyse_sentiment(text):
    # handle null or empty text gracefully
    if not text:
        return "neutral"

    # create a TextBlob object and get polarity score
    blob = TextBlob(text)

    # classify into three buckets based on polarity score
    # polarity ranges from -1.0 (very negative) to 1.0 (very positive)
    if blob.sentiment.polarity > 0.1:
        return "positive"
    elif blob.sentiment.polarity < -0.1:
        return "negative"
    else:
        return "neutral"

# register the Python function as a Spark UDF returning a string
sentiment_udf = udf(analyse_sentiment, StringType())

# read from Kafka using localhost since we are running on Mac directly
# localhost:9092 is the external port mapped from the Kafka container
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("kafka.client.id", "spark-streaming-client") \
    .option("subscribe", "social_posts_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# cast binary Kafka value to string and parse JSON using our schema
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
        col("kafka_timestamp")
    )

# apply sentiment analysis to each post and extract first hashtag from text
enriched_stream = parsed_stream \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("hashtag", regexp_extract(col("text"), r"#\w+", 0))

# group by 2-minute tumbling windows and hashtag, count sentiment buckets
# complete mode outputs the full result table every batch trigger
windowed_counts = enriched_stream \
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

# write results to console every 2 minutes so we can see live sentiment trends
console_query = windowed_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="2 minutes") \
    .option("checkpointLocation", "/Users/mpe/Desktop/Iron Hack/CAPSTONE /Social Media Analytics Platform/spark/streaming/checkpoint") \
    .start()

# block until the stream is manually stopped
console_query.awaitTermination()