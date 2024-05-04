from kafka import KafkaConsumer
import json
from pymongo import MongoClient

topic_names = ['crime']
bootstrap_servers = ['localhost:9092']


consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


client = MongoClient()
db = client['kafka_db']
collection = db['crime']

def subscribe_to_topics(topic_names):
    consumer.subscribe(topic_names)
    for message in consumer:
        
        collection.insert_one(message.value)
        print("Data stored in MongoDB: {}".format(message))

subscribe_to_topics(topic_names)
