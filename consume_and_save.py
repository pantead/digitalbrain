import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "test-topic"
BOOTSTRAP_SERVERS = 'localhost:9092'
FILE_NAME = 'kafka_data.json'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

with open(FILE_NAME, 'w') as file:
    for message in consumer:
        file.write(json.dumps(message.value) + "\n")
