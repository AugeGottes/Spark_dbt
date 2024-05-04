from kafka import KafkaProducer
import requests
import json

bootstrap_servers = ['localhost:9092']


windows_producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))


football_producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                  value_serializer=lambda x: json.dumps(x).encode('utf-8'))
crime_producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                               value_serializer=lambda x: json.dumps(x).encode('utf-8'))



url_windows = 'https://newsapi.org/v2/everything?q=windows&apiKey=673c169fa7a144e6a41050dd6086ef2b'
response_windows = requests.get(url_windows)
topics_windows = response_windows.json()['articles']


# print("Topics for Windows:")
# for topic in topics_windows:
#     print(topic)


for topic in topics_windows:
    windows_producer.send("windows", value=topic)


url_football = 'https://newsapi.org/v2/everything?q=football&apiKey=673c169fa7a144e6a41050dd6086ef2b'
response_football = requests.get(url_football)
topics_football = response_football.json()['articles']


# print("\nTopics for Football:")
# for topic in topics_football:
#     print(topic)

for topic in topics_football:
    football_producer.send("football", value=topic)


url_crime = 'https://newsapi.org/v2/everything?q=crime&apiKey=673c169fa7a144e6a41050dd6086ef2b'
response_crime = requests.get(url_crime)
topics_crime = response_crime.json()['articles']

# print("\nTopics for Crime:")
# for topic in topics_crime:
#     print(topic)


for topic in topics_crime:
    crime_producer.send("crime", value=topic)



windows_producer.close()
football_producer.close()
crime_producer.close()