from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Create SparkSession and StreamingContext
spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 5)

# Create Kafka Consumer instance
topic_names = ['crime']
kafkaParams = {"metadata.broker.list": "localhost:9092"}
stream = KafkaUtils.createDirectStream(ssc, topic_names, kafkaParams, valueDecoder=lambda x: json.loads(x.decode('utf-8')))

# Store data into Spark
stream.foreachRDD(lambda rdd: spark.createDataFrame(rdd).write.mode("append").format("parquet").save("/path/to/output/directory"))

# Start the StreamingContext
ssc.start()
ssc.awaitTermination()
