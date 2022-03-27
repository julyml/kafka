from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import json 
from kafka import KafkaProducer
from ksql import KSQLAPI




   
def load_json_data_kafka(**kwargs):
    topic_name = kwargs.get('topic_name')
    data_path = kwargs.get('data_path')
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0,10,2),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    with open(data_path) as json_file:
        messages = json.load(json_file)

    for message in messages:
        print(f'Producing message @ {datetime.now()} | Message = {str(message)}')
        producer.send(topic_name, message)
    return

def create_table_airflow(**kwargs):
    table_name = kwargs.get('table_name')
    columns_type = kwargs.get('columns_type')
    topic = kwargs.get('topic')
    key = kwargs.get('key')
    value_format = kwargs.get('value_format')
    client = KSQLAPI('http://ksql-server:8088')
    
    try:
        client.create_table(table_name=table_name,
                        columns_type=columns_type,
                        topic=topic,
                        value_format=value_format,
                        key=key)
    except Exception as e:
        print(f'Error at create table: {e}')
    


with DAG(dag_id='load_user_action_data',
         default_args={'owner': 'airflow'},
         schedule_interval="@once",
         start_date=days_ago(2),
         tags=['etl', 'action', 'user']) as dag:

    execution_date = '{{ ds }}'
    
    # load_json_data_kafka = PythonOperator(
    #     task_id='load_json_data_kafka',
    #     python_callable=load_json_data_kafka,
    #     op_kwargs={
    #         'data_path': '/opt/airflow/data/action_data.json',
    #         'topic_name': 'action-data'
    #     }
    # )
    
    create_table = PythonOperator(
        task_id='create_table_users',
        python_callable=create_table_airflow,
        op_kwargs={
            'table_name': 'user_actions',
            'topic': 'action-data',
            "columns_type" : ["id BIGINT", "deviceId VARCHAR", '"timestamp" BIGINT', "action VARCHAR"," videoId BIGINT", "duration BIGINT", "playbackPercentage BIGINT", "playerType VARCHAR", "channel VARCHAR", "title VARCHAR", "displayArtist VARCHAR" ],
            'value_format' : 'JSON',
            "key" : "id"
        }
    )
    
    
    
    create_table




