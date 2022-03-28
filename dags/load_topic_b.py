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
    return  int(time.mktime(datetime.datetime.strptime(str_date,'%m/%d/%Y %H:%M:%S').timetuple()))

def send_data(**kwargs):
    dest_topic_name = kwargs.get('dest_topic_name')
    source_table_name = kwargs.get('source_table_name')
    time_begin = convert(kwargs.get('time_begin'))
    time_end = convert(kwargs.get('time_end'))   
    
    try:
        client = KSQLAPI('http://ksql-server:8088')
        sql = f'select * from user_actions where "timestamp" BETWEEN 1618398000 AND 1618405200;'
        print(sql)
        try:
            query = client.query(sql)
        except Exception as e:
            print(f'Error {e}')
        else:
            for item in query: 
                print('messagem',item)
                load_json_data_kafka(item,dest_topic_name)
    except Exception as e:
            print(f'Error {e}')

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
    
with DAG(dag_id='send_data_topic_b',
         default_args={'owner': 'airflow'},
         schedule_interval="@once",
         start_date=days_ago(2),
         tags=['etl', 'action', 'user']) as dag:

    execution_date = '{{ ds }}'
    
    load_topic_a_data_kafka = PythonOperator(
        task_id='load_topic_a_data_kafka',
        python_callable=send_data,
        op_kwargs={
            'dest_topic_name' : 'topic_b',
            'source_table_name' : 'user_actions',
            'time_begin' : '04/14/2021 11:00:00',
            'time_end' : '04/14/2021 13:00:00'
        }
    )
     
    
    
    load_topic_a_data_kafka
