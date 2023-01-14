from kafka import KafkaConsumer
from multiprocessing import Process
import json
import io
import os

# To modify to your own path
#path = "E:/TSP/M2_DS_22_23/Data_Stream/Lab_Kafka"


def archive_raw_tweets():
    consumer = KafkaConsumer("raw-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('raw_tweets.txt', 'w', encoding="utf-8") as f:
        for tweet in consumer:
            tweet = tweet.value['text']
            print(tweet)
            f.write(f'- {tweet}\n')
    f.close()

def archive_fr_tweets():
    consumer = KafkaConsumer("fr-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('fr_tweets.txt', 'w', encoding="utf-8") as f:
        for tweet in consumer:
            tweet = tweet.value['text']
            print(tweet)
            f.write(f'- {tweet}\n')
    f.close()


def archive_en_tweets():
    consumer = KafkaConsumer("en-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('en_tweets.txt', 'w', encoding="utf-8") as f:
        for tweet in consumer:
            tweet = tweet.value['text']
            f.write(f'- {tweet}\n')
    f.close()
            

def archive_positive_tweets():
    consumer = KafkaConsumer("positive-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('positive_tweets.txt', 'w', encoding="utf-8") as f:
        for tweet in consumer:
            tweet = tweet.value['text']
            f.write(f'- {tweet}\n')
    f.close()
            
            
def archive_negative_tweets():
    consumer = KafkaConsumer("negative-tweets", bootstrap_servers="localhost:9092", group_id="group-1", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    with io.open('negative_tweets.txt', 'w', encoding="utf-8") as f:
        for tweet in consumer:
            tweet = tweet.value['text']
            f.write(f'- {tweet}\n')
    f.close()



############## RUN ARCHIVING ###############
if __name__ == '__main__':
    proc_raw_tweets = Process(target=archive_raw_tweets)
    proc_fr_tweets = Process(target=archive_fr_tweets)
    proc_en_tweets = Process(target=archive_en_tweets)
    proc_pos_tweets = Process(target=archive_positive_tweets)
    proc_neg_tweets = Process(target=archive_negative_tweets)
    proc_raw_tweets.start()
    proc_raw_tweets.e
    proc_fr_tweets.start()
    proc_en_tweets.start()
    proc_pos_tweets.start()
    proc_neg_tweets.start()