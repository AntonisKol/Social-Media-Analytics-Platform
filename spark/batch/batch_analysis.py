# Reads collected Kafka posts and turns them into top hashtag, active user, engagement, and location summaries.
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_extract_all, count, avg, explode, lit
from shared import HASHTAG_PATTERN, post_schema
from pathlib import Path
from datetime import date
import matplotlib.pyplot as plt # for plotting results
import seaborn as sns # for nicer plots

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_posts_raw")
_base_output = os.getenv("BATCH_OUTPUT_DIR", str(Path(__file__).resolve().parent / "output"))
OUTPUT_DIR = os.path.join(_base_output, str(date.today()))
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("SocialMediaBatchAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

posts_df = raw_df \
    .select(from_json(col("value").cast("string"), post_schema).alias("post")) \
    .select(
        col("post.post_id"),
        col("post.user_id"),
        col("post.text"),
        col("post.likes"),
        col("post.retweets"),
        col("post.location")  # enriched by NiFi UpdateAttribute before reaching Kafka
    ) \
    .filter(col("post_id").isNotNull())

posts_df = posts_df.withColumn(
    "hashtag", explode(regexp_extract_all(col("text"), lit(HASHTAG_PATTERN), 0))
)

posts_df = posts_df.withColumn(
    "engagement", col("likes") + col("retweets")
)

top_hashtags = posts_df \
    .filter(col("hashtag") != "") \
    .groupBy("hashtag") \
    .agg(count("*").alias("post_count")) \
    .orderBy(col("post_count").desc()) \
    .limit(10)

top_hashtags.show(truncate=False)
top_hashtags.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_DIR}/top_hashtags")

top_hashtags_pd = top_hashtags.toPandas()
plt.figure(figsize=(10, 6))
sns.barplot(data=top_hashtags_pd, x="post_count", y="hashtag", hue="hashtag", palette="Blues_d", legend=False)
plt.title("Top 10 Hashtags by Post Count")
plt.xlabel("Number of Posts")
plt.ylabel("Hashtag")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/top_hashtags.png")
plt.close()

active_users = posts_df \
    .groupBy("user_id") \
    .agg(count("*").alias("post_count")) \
    .orderBy(col("post_count").desc()) \
    .limit(10)

active_users.show(truncate=False)
active_users.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_DIR}/active_users")

active_users_pd = active_users.toPandas()
plt.figure(figsize=(10, 6))
sns.barplot(data=active_users_pd, x="post_count", y="user_id", hue="user_id", palette="Greens_d", legend=False)
plt.title("Top 10 Most Active Users")
plt.xlabel("Number of Posts")
plt.ylabel("User ID")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/active_users.png")
plt.close()

# Calculate average engagement (likes + retweets) per hashtag and show top 10
engagement_by_hashtag = posts_df \
    .filter(col("hashtag") != "") \
    .groupBy("hashtag") \
    .agg(
        count("*").alias("post_count"),
        avg("engagement").alias("avg_engagement")
    ) \
    .orderBy(col("avg_engagement").desc()) \
    .limit(10)

engagement_by_hashtag.show(truncate=False)
engagement_by_hashtag.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_DIR}/engagement_by_hashtag")

engagement_pd = engagement_by_hashtag.toPandas()
plt.figure(figsize=(10, 6))
sns.barplot(data=engagement_pd, x="avg_engagement", y="hashtag", hue="hashtag", palette="Oranges_d", legend=False)
plt.title("Average Engagement (Likes + Retweets) per Hashtag")
plt.xlabel("Average Engagement")
plt.ylabel("Hashtag")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/engagement_by_hashtag.png")
plt.close()

posts_by_location = posts_df \
    .filter(col("location").isNotNull()) \
    .groupBy("location") \
    .agg(
        count("*").alias("post_count"),
        avg("engagement").alias("avg_engagement")
    ) \
    .orderBy(col("post_count").desc())

posts_by_location.show(truncate=False)
posts_by_location.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_DIR}/posts_by_location")

location_pd = posts_by_location.toPandas()
plt.figure(figsize=(12, 6))
sns.barplot(data=location_pd, x="post_count", y="location", hue="location", palette="Purples_d", legend=False)
plt.title("Post Count by City")
plt.xlabel("Number of Posts")
plt.ylabel("City")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/posts_by_location.png")
plt.close()

print("Batch analysis complete. CSVs and plots saved to:", OUTPUT_DIR)
spark.stop()
