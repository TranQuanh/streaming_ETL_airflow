from kafka.Consumer import ConsumerClass
from kafka.Producer import ProducerClass

def pull_message(): 
    group_id = 'kha'
    topics = ['spark']
    consumer = ConsumerClass(group_id,topics)
    msgs = consumer.consume_message()
    return msgs
def push_message(msgs):
    topics_airflow = 'airflow'
    producer = ProducerClass(topics_airflow)
    for msg in msgs:
        print(msg)
        producer.send_message(msg)
    producer.commit()