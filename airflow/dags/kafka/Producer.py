from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import socket
import json
def acked(err, msg):
        if err is not None:
            # print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            print("Failed to deliver message: %s" % str(err))
        else:
            # print("Message produced: %s" % (str(msg)))
            ("load data successfully")
class ProducerClass:
    producer_msg_count = 0
    MIN_FLUSH_COUNT = 500
    def __init__(self,topic):
        self.topic = topic
        conf = {
        # 'bootstrap.servers': bootstrap_server,    
        'bootstrap.servers': os.getenv('PRODUCER_BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('PRODUCER_SECURITY_PROTOCOL','SASL_PLAINTEXT'),
        'sasl.mechanism': os.getenv('PRODUCER_SASL_MECHANISM','PLAIN'),
        'sasl.username': os.getenv('PRODUCER_SASL_USERNAME'),
        'sasl.password': os.getenv('PRODUCER_SASL_PASSWORD'),
        # 'client.id': socket.gethostname(),
        # 'acks': 'all',
        # 'enable.idempotence': True,
        # 'retries': 5,
        }
        self.producer = Producer(**conf)
    def send_message(self,msg):
        
        self.producer.produce(
            self.topic,
            value=msg.encode('utf-8')
            # callback = acked
        )
        ("load data")
    def commit(self):
        self.producer.flush()