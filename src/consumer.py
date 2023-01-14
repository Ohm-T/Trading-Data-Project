from multiprocessing import Process
from kafka import KafkaConsumer
import json

class KafkaWrapper():
    """
    Wrapper Consumers to multiprocess it
    """
    def __init__(self, topic_name):
        self.consumer = KafkaConsumer(topic_name, bootstrap_servers = 'my.server.com',group='group-1', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        #group_id="group-1"


    def consume(self, topic):
        self.consumer.subscribe(topic)
        for message in self.consumer:
            print(message.value)

class ServiceInterface():
    def __init__(self):
        self.kafka_wrapper = KafkaWrapper()

    def start(self, topic):
        self.kafka_wrapper.consume(topic)

class ServiceA(ServiceInterface):
   pass

class ServiceB(ServiceInterface):
   pass

def main():
    serviceA = ServiceA()
    serviceB = ServiceB()
    jobs = []
    jobs.append(Process(target = serviceA.start, args = ("my-topic", )))
    jobs.append(Process(target = serviceB.start, args = ("my-topic", )))

    for job in jobs:
        job.start()

    for job in jobs:
        job.join()

