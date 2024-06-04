import pandas as pd 
import json
from kafka import KafkaProducer

counter = 0

# Инициализация продьюсера
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for chunk in pd.read_csv('market_analysis.csv', delimiter=';', chunksize=100):

    dict_to_kafka = chunk.to_dict()
    data = json.dumps(dict_to_kafka, default=str).encode('utf-8')

    key = str(counter).encode()

    # заливка данных в кафку
    producer.send(topic="market_analysis", key=key, value=data)

    counter = counter + 1
