from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer,Producer
from datetime import datetime, timedelta
import json

from kafka.Consumer import ConsumerClass
from kafka.Producer import ProducerClass

def pull_message(): 
    group_id = 'kha'
    topics = ['spark']
    consumer = ConsumerClass(group_id,topics)
    msgs = consumer.consume_message()
    return msgs
def push_message(ti):
    # topic = 'airflow'
    # msgs = ti.xcom_pull(task_ids = 'consume_messages')
    # producer = ProducerClass(topic)
    # for msg in msgs:
    #     print(msg)
    #     producer.send_message(msg)
    # producer.commit()

    conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-0:9092,kafka-2:9092',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'admin',
        'sasl.password': 'Unigap@2024',
    }
    topic = 'airflow'
    producer = ProducerClass(topic)
    
    msgs = ti.xcom_pull(task_ids = 'consume_messages')
    for msg in msgs:       
        producer.send_message(msg)
    producer.commit()
            
    return msgs
default_args = {
    'owner': 'quanganh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='produce_topic',
    default_args=default_args,
    description='DAG to consume messages from a spark topic and produce to airflow topic',
    start_date=datetime(2024, 11, 17),
    schedule_interval='@daily'
) as dag:
    
    consume_task = PythonOperator(
        task_id='consume_messages',
        python_callable=pull_message,
        provide_context=True,
    )
    produce_task = PythonOperator(
        task_id='produce_messages',
        python_callable=push_message,
        provide_context=True
    )
    consume_task >> produce_task