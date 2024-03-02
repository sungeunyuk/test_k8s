from kafka import KafkaProducer
from json import dumps
import time

from time import sleep
 
producer = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda x:dumps(x).encode('utf-8')
)
 
start = time.time()
 
for i in range(1000000000000):
    sleep(0.15)
    data = {'text' : 'result'+str(i)}
    producer.send(topic='iis_log', value=data)
    producer.flush()
 
print('[Done]:', time.time() - start)