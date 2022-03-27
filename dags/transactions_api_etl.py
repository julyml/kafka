
import datetime
from datetime import datetime as dt
from pkgutil import get_data
import pendulum
import sys
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

def save_raw_data(conf):
    response = requests.request("GET", conf['url'])
    with open(f"{conf['save_to']}/{conf['dataset']}.csv", "w") as file:
        file.write(response.text)

def load_to_db(conf):
    postgres_hook = PostgresHook(postgres_conn_id="pipelines")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(f"{conf['save_to']}/{conf['dataset']}.csv", "r") as file:
        cur.copy_expert(
            f"COPY {conf['database']} FROM STDIN WITH CSV HEADER DELIMITER AS ','",
            file,
        )
    conn.commit()

@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def Etl():
    @task
    def load_data_expert_level():
        save_raw_data(c2019_exp)        
        load_to_db(c2019_exp)

    @task
    def load_data_avg():
        save_raw_data(c2019_avg)        
        load_to_db(c2019_avg)

    load_data_expert_level() >> load_data_avg()


dag = Etl()