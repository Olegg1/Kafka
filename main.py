from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer


class Main:
    def __init__(self) -> None:
        self.admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='test'
        )

    def create(self, name):
        topic_list = []

        # добавления нового топика
        topic_list.append(NewTopic(name=name, num_partitions=1, replication_factor=1))

        # создание топика
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print('Успех')
    def dell(self, name):
        self.admin_client.delete_topics(topics=[name])

    def insert(self, df, topic_name):
        counter = 0

        # Инициализация продьюсера
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        for chunk in df.to_dict(orient='records'):
            data = json.dumps(chunk, default=str).encode('utf-8')
            key = str(counter).encode()
            producer.send(topic=topic_name, key=key, value=data)
            counter += 1

    def read(self, name):
        consumer = KafkaConsumer(name,
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: m.decode('utf-8'),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

        out_list = []
        for message in consumer:
            print(message.value)
            out_list.append(message.value)
        
        df = pd.DataFrame(out_list)
        return df


topic = Main()
# topic.dell('name')
# topic.create('name')
topic.read('name')