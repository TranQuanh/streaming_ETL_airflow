
from confluent_kafka import Consumer

topics = ['spark']
topic = 'spark'
conf = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
    'group.id':'kha',

    'retries': 5,
    'auto.offset.reset':  'earliest',
}

consumer = Consumer(**conf)
consumer.subscribe(topics)
msgs = consumer.consume(10,timeout=20)
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
            print(msg_value)
            list_msg.append(msg_value)
print(list_msg)