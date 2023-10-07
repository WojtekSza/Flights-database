import logging
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id="Flights Database",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    @task(task_id="print_the_context")
    def print_context():
        print("test")
        return "Whatever you return gets printed in the logs"
    run_this = print_context()