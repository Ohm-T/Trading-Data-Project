from kafka import KafkaConsumer
from datetime import datetime


topic_raw = "raw-tweets"
topic_fr="fr-tweets"
topic_en="en-tweets"
topic_pos = "positive-tweets"
topic_neg = "negative-tweets"
topics = (
    topic_raw, topic_fr, topic_en, topic_pos, topic_neg
)

def main():
    consumer = KafkaConsumer(*topics, bootstrap_servers="localhost:9092", group_id="group-1")
    for msg in consumer:
        print(f"Topic-name: {msg.topic}")
        print(f"Partition-id: {msg.partition}")
        print(f"Offset-id: {msg.offset}\n")
        print(f"Timestamp: {datetime.now()}")


if __name__=="__main__":
    main()