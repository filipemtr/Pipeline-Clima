"""
Pipeline que extrái dados do clima de um local determinado no arquivo .env,
trata deles e os envia para um banco de dados PostgreSQL.
"""

from datetime import datetime as date, timedelta
from airflow.decorators import task, dag
from dotenv import load_dotenv
import os
import sys

from src.extract import extractdata
from src.transform import transformdata
from src.load import loaddata

load_dotenv()
sys.path.append('/opt/airflow')

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "email": os.getenv('EMAIL'),
    "email_on_failure": True
}

@dag(
    dag_id="etl_pipeline",
    schedule="@hourly",
    start_date=date(2026, 3, 15),
    catchup=False,
    default_args=default_args,
    tags=["etl", "pipeline"],
)
def etl_pipeline():

    @task
    def extract():
        return extractdata()

    @task
    def transform(df):
        return transformdata(df)

    @task
    def load(df):
        loaddata(df)

    df = extract()
    df_transformado = transform(df)
    load(df_transformado)


dag = etl_pipeline()




    