from multiprocessing
import Process

class KafkaWrapper():
   def __init__(self):
   self.consumer = KafkaConsumer(bootstrap_servers = 'my.server.com')

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
# The code works fine
if I used threading.Thread here instead of Process
jobs.append(Process(target = serviceA.start, args = ("my-topic", )))
jobs.append(Process(target = serviceB.start, args = ("my-topic", )))

for job in jobs:
   job.start()

for job in jobs:
   job.join()

if __name__ == "__main__":
   main()
