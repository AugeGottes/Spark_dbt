from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


topic_names = ['crime']
bootstrap_servers = ['localhost:9092']


schema = StructType([
    StructField("status", StringType(), True),
    StructField("totalResults", DoubleType(), True),
    StructField("articles", StructType([
        StructField("source", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("author", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("urlToImage", StringType(), True),
        StructField("publishedAt", TimestampType(), True),
        StructField("content", StringType(), True)
    ]), True)
])


spark = SparkSession.builder.appName("KafkaStream").getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(bootstrap_servers)) \
    .option("subscribe", ",".join(topic_names)) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.articles.author", "data.articles.title") \
    .filter(col("data.articles.author").startswith("M"))


df.writeStream \
    .format("memory") \
    .queryName("my_table") \
    .start() \
    .awaitTermination()


result = spark.sql("SELECT * FROM my_table")


result.show()
