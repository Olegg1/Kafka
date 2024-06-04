from kafka.admin import KafkaAdminClient, NewTopic


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='test'
)

# создание списка топиков
topic_list = []

# добавления нового топика
topic_list.append(NewTopic(name="market_analysis", num_partitions=1, replication_factor=1))

# создание топика
admin_client.create_topics(new_topics=topic_list, validate_only=False)

# удаление топика
# admin_client.delete_topics(topics=['market_analysis'])