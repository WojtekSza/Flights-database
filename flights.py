import logging
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
import pandas as pd, numpy as np
import requests
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

with DAG(
    dag_id="flights_database",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    @task(task_id="print_the_context")
    def print_context():
        # -*- coding: utf-8 -*-


        url = "https://airlabs.co/api/v9/flights?api_key=9bbe822a-4885-4327-aa03-d363f1282adf"
        response = requests.request("GET", url)
        result=response.json()

        api_flights_df = pd.DataFrame(result['response']) # Compute a Pandas dataframe to write into api_flights
        api_flights_df['updated']=pd.to_datetime(api_flights_df['updated'], unit='s')

        db_params = {
            'dbname': 'flights',
            'user': 'reader',
            'password': '***',
            'host': '3.126.41.236',  # or the hostname of your PostgreSQL server
            'port': '5432',       # default PostgreSQL port
        }

        df = pd.DataFrame(api_flights_df)

        # Create a SQLAlchemy engine
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}')

        try:
            # Write the DataFrame to the SQL table
            df.to_sql('flight_data', engine, if_exists='append', index=False)

        except Exception as e:
            print("Error writing DataFrame to the SQL table:", e)

        finally:
            engine.dispose()

        return "Whatever you return gets printed in the logs"
    
    run_this = print_context()
