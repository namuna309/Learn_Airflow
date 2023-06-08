from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import requests
import pandas as pd
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_contries_data(url):
    res = requests.get(url)
    datas = res.json()
    records = []

    for data in datas:
        name = data['name']['common']
        official = data['name']['official']
        if "'" in official:
            official = official.replace("'", "''")
        population = data['population']
        area = data['area']
        records.append([name, official, population, area])

    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    name varchar(100),
    official varchar(1000),
    population bigint,
    area float
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}','{r[1]}', {r[2]}, {r[3]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'UpdateContries',
    start_date = datetime(2023,4,30,hour=6,minute=30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    url = 'https://restcountries.com/v3.1/all?fields=name,official,population,area'
    results = get_contries_data(url)
    load("nyah309", "contry_info", results)