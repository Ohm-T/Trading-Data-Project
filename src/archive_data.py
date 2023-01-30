from kafka import KafkaConsumer
from multiprocessing import Process
import json
import io
import os

# To modify to your own path
#path = "E:/TSP/M2_DS_22_23/Data_Stream/Lab_Kafka"


def archive_stock():
    consumer = KafkaConsumer("raw-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('stock_prediction.txt', 'w', encoding="utf-8") as f:
        for stock in consumer:
            stock = stock.value['text']
            print(stock)
            f.write(f'- {stock}\n')
    f.close()

if __name__ == '__main__':
    proc_stock = Process(target=archive_stock)
