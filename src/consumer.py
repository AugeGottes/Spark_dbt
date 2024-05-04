import urllib.request
import json
from pyspark.sql import SparkSession, DataFrame, SQLContext


url = "https://newsapi.org/v2/everything?q=crime&apiKey=673c169fa7a144e6a41050dd6086ef2b"
response = urllib.request.urlopen(url).read().decode()
data = json.loads(response)

spark = SparkSession.builder \
    .appName("NewsAPI") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(data['articles'])

df.show()
