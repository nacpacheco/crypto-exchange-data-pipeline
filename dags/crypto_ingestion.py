import json
import logging
import os
import time

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from airflow.operators.empty import EmptyOperator

BITSO_SAMPLE_MARKETS = [('XRP','USD'), ('BTC','USD'), ('ETH','USD')]
RAW_OUTPUT_PATH = "/opt/airflow/data/crypto_ingestion/raw/"
API_HEADERS = {
    "accept": "application/json",
    "x-cg-demo-api-key": Variable.get("COINGECKO_API_KEY")
}

def get_exchanges(execution_date):
    url = "https://api.coingecko.com/api/v3/exchanges?per_page=50&page=1"
    response = requests.get(url, headers=API_HEADERS)
    os.makedirs(f"{RAW_OUTPUT_PATH}/exchanges/date_ingested={execution_date}", exist_ok=True)
    with open(f"{RAW_OUTPUT_PATH}/exchanges/date_ingested={execution_date}/exchanges.json", "w") as f:
        json.dump(response.json(), f)

def get_shared_makets(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/exchanges/date_ingested={execution_date}/exchanges.json") as f:
        exchanges = json.load(f)
    shared_markets_list = []
    for exchange in exchanges:
        logging.info(f"Processing exchange: {exchange['id']}")
        tickers_url = f"https://api.coingecko.com/api/v3/exchanges/{exchange['id']}/tickers"
        tickers_response = requests.get(tickers_url, headers=API_HEADERS)
        if tickers_response.status_code != 200:
            logging.error(f"Failed to fetch tickers for exchange {exchange['id']}: {tickers_response.status_code}")
            continue
        else:
            for ticker in tickers_response.json().get('tickers', []):
                base = ticker.get('base')
                target = ticker.get('target')
                if (base, target) in BITSO_SAMPLE_MARKETS:
                    shared_markets_list.append(ticker)
        time.sleep(2) # To avoid hitting API rate limits

    os.makedirs(f"{RAW_OUTPUT_PATH}/shared_markets/date_ingested={execution_date}", exist_ok=True)
    with open(f"{RAW_OUTPUT_PATH}/shared_markets/date_ingested={execution_date}/shared_markets.json", "w") as f:
        json.dump(shared_markets_list, f)

def get_exchange_30day_volume(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/exchanges/date_ingested={execution_date}/exchanges.json") as f:
        exchanges = json.load(f)
    results = []
    for exchange in exchanges:
        url = f"https://api.coingecko.com/api/v3/exchanges/{exchange['id']}/volume_chart?days=30"
        resp = requests.get(url, headers=API_HEADERS)
        if resp.status_code != 200:
            logging.error(f"Failed to fetch volume chart for exchange {exchange['id']}: {resp.status_code}")
            continue
        else:
            results.append({
                "exchange_id": exchange['id'],
                "volume_chart": resp.json()
            })
        time.sleep(2)  # To avoid hitting API rate limits
    os.makedirs(f"{RAW_OUTPUT_PATH}/exchange_30day_volume/date_ingested={execution_date}", exist_ok=True)
    with open(f"{RAW_OUTPUT_PATH}/exchange_30day_volume/date_ingested={execution_date}/exchange_30day_volume.json", "w") as f:
        json.dump(results, f)

def get_market_30day_volume(execution_date):
    with open(f"{RAW_OUTPUT_PATH}/shared_markets/date_ingested={execution_date}/shared_markets.json") as f:
        markets = json.load(f)
        print(markets)

    unique_markets = {
        (market.get("base"), market.get("target"), market.get("coin_id"))
        for market in markets
    }

    results = []
    for base, target, coin_id in unique_markets:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency={target}&days=30&interval=daily"
        resp = requests.get(url, headers=API_HEADERS)
        results.append({
            "market_id": f"{base}_{target}",
            "market_chart": resp.json()
        })
        time.sleep(2) # To avoid hitting API rate limits
    os.makedirs(f"{RAW_OUTPUT_PATH}/market_30day_volume/date_ingested={execution_date}", exist_ok=True)
    with open(f"{RAW_OUTPUT_PATH}/market_30day_volume/date_ingested={execution_date}/market_30day_volume.json", "w") as f:
        json.dump(results, f)



with DAG(
    dag_id="crypto_ingestion",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="fetch_exchanges",
        python_callable=get_exchanges,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t2 = PythonOperator(
        task_id="fetch_shared_markets",
        python_callable=get_shared_makets,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t3 = PythonOperator(
        task_id="fetch_exchange_30day_volume",
        python_callable=get_exchange_30day_volume,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t4 = PythonOperator(
        task_id="fetch_market_30day_volume",
        python_callable=get_market_30day_volume,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    t5 = EmptyOperator(
        task_id="end",
        outlets=[Dataset("/dataset/crypto_data")]
    )


    t1 >> t2 >> t3 >> t4 >> t5
