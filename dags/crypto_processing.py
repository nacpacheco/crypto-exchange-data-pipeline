import json
import os

import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

RAW_OUTPUT_PATH = "/opt/airflow/data/crypto_ingestion/raw"
PROCESSED_OUTPUT_PATH = "/opt/airflow/data/crypto_ingestion/processed"

def process_exchange_table(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/exchanges/date_ingested={execution_date}/exchanges.json") as f:
        exchanges = json.load(f)

    df_exchanges = pd.DataFrame([{
        "exchange_id": exchange["id"],
        "exchange_name": exchange["name"],
        "year_established": int(exchange.get("year_established")),
        "country": exchange.get("country"),
        "trust_score": int(exchange.get("trust_score")),
        "trust_score_rank": int(exchange.get("trust_score_rank"))
    } for exchange in exchanges])
    print(df_exchanges)

    os.makedirs(f"{PROCESSED_OUTPUT_PATH}/exchanges/date={execution_date}", exist_ok=True)
    df_exchanges.to_parquet(f"{PROCESSED_OUTPUT_PATH}/exchanges/date={execution_date}/data.parquet")


def process_shared_markets_table(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/shared_markets/date_ingested={execution_date}/shared_markets.json") as f:
        shared_markets = json.load(f)

    df_shared_markets = pd.DataFrame([{
        "exchange_id": ticker["market"]["identifier"],
        "market_id": f"{ticker['base']}_{ticker['target']}",
        "base": ticker["base"],
        "target": ticker["target"],
        "name": f"{ticker['base']}/{ticker['target']}"
    } for ticker in shared_markets])
    print(df_shared_markets)

    os.makedirs(f"{PROCESSED_OUTPUT_PATH}/shared_markets/date={execution_date}", exist_ok=True)
    df_shared_markets.to_parquet(f"{PROCESSED_OUTPUT_PATH}/shared_markets/date={execution_date}/data.parquet")

def process_exchange_30day_volume(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/exchange_30day_volume/date_ingested={execution_date}/exchange_30day_volume.json") as f:
        exchange_volumes = json.load(f)
    records = []
    for item in exchange_volumes:
        exchange_id = item["exchange_id"]
        for timestamp, volume in item["volume_chart"]:
            records.append({
                "exchange_id": exchange_id,
                "timestamp": str(pd.to_datetime(timestamp, unit='ms')),
                "volume": float(volume)
            })

    df_exchange_volume = pd.DataFrame(records)
    print(df_exchange_volume)

    os.makedirs(f"{PROCESSED_OUTPUT_PATH}/exchange_30day_volume/date={execution_date}", exist_ok=True)
    df_exchange_volume.to_parquet(f"{PROCESSED_OUTPUT_PATH}/exchange_30day_volume/date={execution_date}/data.parquet")

def process_market_30day_volume(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/market_30day_volume/date_ingested={execution_date}/market_30day_volume.json") as f:
        market_volumes = json.load(f)

    records = []
    for item in market_volumes:
        market_id = item["market_id"]
        for timestamp, volume in item["market_chart"]["total_volumes"]:
            records.append({
                "marked_id": market_id,
                "timestamp": str(pd.to_datetime(timestamp, unit='ms')),
                "volume": float(volume)
            })

    os.makedirs(f"{PROCESSED_OUTPUT_PATH}/market_30day_volume/date={execution_date}", exist_ok=True)
    df_market_volume = pd.DataFrame(records)
    print(df_market_volume)
    df_market_volume.to_parquet(f"{PROCESSED_OUTPUT_PATH}/market_30day_volume/date={execution_date}/data.parquet")


with DAG(
    dag_id="crypto_processing",
    start_date=datetime(2025, 8, 1),
    schedule=[Dataset("/dataset/crypto_data")],
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="generate_exchanges_table",
        python_callable=process_exchange_table,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t2 = PythonOperator(
        task_id="generate_shared_markets_table",
        python_callable=process_shared_markets_table,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t3 = PythonOperator(
        task_id="generate_exchange_30day_volume_table",
        python_callable=process_exchange_30day_volume,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t4 = PythonOperator(
        task_id="generate_market_30day_volume_table",
        python_callable=process_market_30day_volume,
        op_kwargs={"execution_date": "{{ ds }}"}
    )