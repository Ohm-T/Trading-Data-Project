import os
from topic_management import topic_management
from ingest_data import *
from monitor_kafka import monitor
from river_models import read_and_learn, predict
from multiprocessing import Process, Queue

def delete_topics(list_topic):
    for topic in list_topic:
        os.popen(f'kafka-topics.bat --delete --topic {topic} --bootstrap-server localhost:9092')
    for topic in list_topic:
        os.popen(f'kafka-topics.bat --delete --topic pred_{topic} --bootstrap-server localhost:9092')

def check_list_topics():
    os.popen('kafka-topics.bat --bootstrap-server localhost:9092 --list')


# def multi_run(ingest_data, list_train, read_and_learn, topic_test):
#     q = Queue()
#     procs = []
#     rets = dict()
#     p = Process(target=ingest_data, args=(list_train, list_tickers))
#     procs.append(p)
#     p.start()
#     for topic in topic_test:
#         p = Process(target=read_and_learn, args=(q, topic))
#         procs.append(p)
#         p.start()
#         ret = q.get()
#         rets[topic] = ret
    
#     for p in procs:
#         p.join()
#     return rets

def multi_run(ingest_data, list_train, read_and_learn, topic_test):
    q = Queue()
    procs = []
    p = Process(target=ingest_data, args=(list_train, list_tickers))
    procs.append(p)
    p.start()
    p = Process(target=read_and_learn, args=(q, topic_test))
    procs.append(p)
    p.start()
    ret = q.get()
    for p in procs:
        p.join()
    return ret



list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']
topic_test = 'goog'

if __name__ == "__main__":
    # Create topics if needed
    topic_management(list_tickers)
    # Load data
    dict_companies = load_tickers(list_tickers)
    #lists_train = [{'GOOG':dataframe, 'AAPL': ..., ...] for 420 first values (train)
    list_train, list_test = dict_companies['train'], dict_companies['test']

    # Send train data to Topics and get the data from kafka consumer and update river model
    model_trained = multi_run(producer_data, list_train, read_and_learn, topic_test)
    prediction = predict(model_trained, horizon = 100)
    print(prediction)
"""
real_value = list_test['GOOG']
pred = [120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884, 120.34201892973884]"""

