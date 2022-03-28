from datetime import datetime
from email import message
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import json 
from kafka import KafkaProducer
from ksql import KSQLAPI



import time
import datetime

def convert(str_date):
    return time.mktime(datetime.datetime.strptime(str_date,'%d/%m/%y %H:%M:%S').timetuple())

def send_data(**kwargs):
    dest_topic_name = kwargs.get('dest_topic_name')
    source_topic_name = kwargs.get('source_topic_name')
    time_begin = kwargs.get('data_path')
    time_end = kwargs.get('data_path')
    
    client = KSQLAPI('http://ksql-server:8088')
    query = client.query(f'select * from {source_topic_name} where "timestamp" BETWEEN {time_begin} AND {time_end}')
    for item in query: 
        load_json_data_kafka(item,dest_topic_name)
    return
        
def load_json_data_kafka(message,dest_topic_name):
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0,10,2),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f'Producing message @ {datetime.now()} | Message = {str(message)}')
    producer.send(dest_topic_name, message)
    return
    
with DAG(dag_id='load_user_action_data',
         default_args={'owner': 'airflow'},
         schedule_interval="@once",
         start_date=days_ago(2),
         tags=['etl', 'action', 'user']) as dag:

    execution_date = '{{ ds }}'
    
    load_topic_a_data_kafka = PythonOperator(
        task_id='load_json_data_kafka',
        python_callable=send_data,
        op_kwargs={
            'topic_name' = 'topic_b'
            'time_begin' =  convert('14/04/2021 11:00:00')
            'time_end' = convert('14/04/2021 16:00:00')
        }
    )
     
    
    
    load_json_data_kafka
