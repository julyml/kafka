from kafka import KafkaConsumer
from json import loads
from datetime import datetime
from time import sleep
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import json 
from kafka import KafkaProducer


def consume_topic(**kwargs):
    topic_src = kwargs.get('topic_src')
    topic_tgt = kwargs.get('topic_tgt')
    
    consumer = KafkaConsumer(
        topic_src,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
     )

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            break
        if msg == '':
            continue
        load_json_data_kafka(msg,topic_tgt)
        print(type(msg))
        sleep(3)
        

def load_json_data_kafka(message, topic_name):
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0,10,2),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )    
    producer.send(topic_name, message)
    producer.flush()
    return

with DAG(dag_id='consume_topic_a',
         default_args={'owner': 'airflow'},
         schedule_interval="@once",
         start_date=days_ago(2),
         tags=['etl', 'action', 'user']) as dag:

    execution_date = '{{ ds }}'
    
    consume_topic_a = PythonOperator(
        task_id='consume_topic_a',
        python_callable=consume_topic,
        op_kwargs={
            'topic_src': 'topic_a',
            'topic_tgt':'topic_b'
        }
    )
        
    consume_topic_a