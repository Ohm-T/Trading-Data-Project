import os
from topic_management import topic_management
from ingest_data import *
from monitor_kafka import monitor
from river_models import read_and_predict

def delete_topics(list_topic):
    for topic in list_topic:
        os.popen(f'kafka-topics.bat --delete --topic {topic} --bootstrap-server localhost:9092')
    for topic in list_topic:
        os.popen(f'kafka-topics.bat --delete --topic pred_{topic} --bootstrap-server localhost:9092')

def check_list_topics():
    os.popen('kafka-topics.bat --bootstrap-server localhost:9092 --list')


list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']
# Create topics if needed
topic_management(list_tickers)
# Load data
data = load_tickers(list_tickers)
# Send data to Topics
ingest_data(data)
# Get the data from kafka consumer and update river model
topic_test = 'goog'
#monitor(topic_test)
# Send the classification to Topics prediction
prediction = read_and_predict(topic_test)
print(prediction)
