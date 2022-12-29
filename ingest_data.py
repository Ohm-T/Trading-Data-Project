from kafka import KafkaProducer
import datetime 
import time
import json

bearer_token = ''
topic_name = ''

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)

while True:
    datas = ''

    for data in datas:
        data = {" ": ''}
        producer.send(topic_name, data)
        print(data)
        print('\n')
    time.sleep(10)

