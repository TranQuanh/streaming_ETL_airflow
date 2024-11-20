from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from dotenv import load_dotenv
import os
import socket
import sys
import logging
from confluent_kafka import KafkaError
import json
import time
load_dotenv()
logging.basicConfig(filename='kafka_errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ConsumerClass:

    # create consumer
    def __init__(self,group_id,topics):
        self.group_id = group_id
        self.topics = topics
        self.topic = topics[0]
        conf = {
        'bootstrap.servers':os.getenv('BOOTSTRAP_SERVERS'),
        # 'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanism': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'client.id': socket.gethostname(),
        # # 'group.id': 'behaviour', #load mongodb - 1 consumer
        # 'group.id': 'behaviour_topic', #load consumer - 1 consumer
        # 'group.id': os.getenv('GROUP_ID'),
        'group.id':group_id, 
        'enable.auto.commit': True,
        'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest')
        }
        self.consumer = Consumer(**conf)
        self.consumer.subscribe(topics)

    # process batch consume
    #  return 10 message in list
    def consume_message(self):
        msgs = self.consumer.consume(10,timeout=20)
        list_msg = []
        for msg in msgs:
                
                if msg is None: 
                    print("không co j")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        #End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                        continue
                    elif msg.error():
                        logging.error(f"Kafka Error: {msg.error()}")

                else:
                    msg_value = msg.value().decode('utf-8')
                    list_msg.append(msg_value)
                    
        return list_msg
    # commit offset 
    def commit(self):
         self.consumer.commit(asynchronous=False)
    # committed offset in kafka
    def check_offsets(self):
        partitions = [TopicPartition(self.topic, i) for i in range(3)]  # Giả sử topic có 3 partition
        # Lấy committed offset cho tất cả các partition
        committed_offsets = self.consumer.committed(partitions)

        # In ra committed offset cho từng partition
        for partition, committed_offset in zip(partitions, committed_offsets):
            if committed_offset is not None:
                print(f"Partition {partition.partition}: Offset đã tiêu thụ: {committed_offset.offset}")
            else:
                print(f"Partition {partition.partition}: Chưa có offset nào được tiêu thụ.")
    # len of partition consumer consume            
    def len_partition_in_consumer(self):
        for i in range(10):
            msg = self.consumer.poll(1.0)
            time.sleep(2) 
        time.sleep(2) 
        partitions_consumer = self.consumer.assignment() # Chuyển thành chuỗi để in ra
        print(f" Consumer is managing partitions: {len(partitions_consumer)}")
    
    def close(self):
        self.consumer.close()
# bootstrap_server = '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294'
# group_id = 'final'
# topics = ['product_view']
# consumer = ConsumerClass(bootstrap_server,group_id,topics)
# print(consumer.consume_message())
