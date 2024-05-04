from kafka import KafkaConsumer
import json


topic_names = ['crime']

bootstrap_servers = ['localhost:9092']


consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def subscribe_to_topics(topic_names):
    consumer.subscribe(topic_names)
    for message in consumer:
        print(message)

subscribe_to_topics(topic_names)
