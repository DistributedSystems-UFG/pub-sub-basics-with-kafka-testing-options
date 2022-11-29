from kafka import KafkaProducer
from const import *
import sys

try:
    topic = sys.argv[1]
    key1 = sys.argv[2]
except:
    print ('Usage: python3 producer <topic_name> <key>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    msg = 'My ' + str(i) + 'st message for topic ' + topic
    print ('Sending message: ' + msg)
    producer.send(topic, value=msg.encode(), key=key1.encode())

producer.flush()
