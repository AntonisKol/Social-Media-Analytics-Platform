from pyspark.sql.types import StructType, StructField, StringType, IntegerType

HASHTAG_PATTERN = r"#\w+"

post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("retweets", IntegerType(), True),
    StructField("location", StringType(), True)
])
