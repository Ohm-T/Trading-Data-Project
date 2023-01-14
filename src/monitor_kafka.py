from kafka import KafkaConsumer
from datetime import datetime
import json
import sys

def monitor(topic_name):
    consumer = KafkaConsumer(topic_name, bootstrap_servers="localhost:9092", group_id='group-1', value_deserializer=lambda m: json.loads(m.decode()))
    for msg in consumer:
        print(f"Topic-name: {msg.topic}")
        print(f"Partition-id: {msg.partition}")
        print(f"Offset-id: {msg.offset}\n")
        print(f"Timestamp: {datetime.now()}")
    sys.exit()