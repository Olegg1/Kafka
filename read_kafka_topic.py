from kafka import KafkaConsumer
import json
import pandas as pd

consumer = KafkaConsumer('market_analysis',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

out_list = []

for message in consumer:
    out_list.append(message)

    print(out_list)

print(pd.DataFrame(message).info())