from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import json
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = "stock-market"

def _get_minio_client():
    minio_conn = BaseHook.get_connection("minio_conn")
    return Minio(
        endpoint=minio_conn.extra_dejson["endpoint_url"].split('//')[1],
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False
    )

def _get_stock_prices(url: str, symbol: str) -> dict:
    import requests

    url = f"{url}{symbol}?metrics=hight?&interval=1d&range=1y"
    api_conn = BaseHook.get_connection("yahoo_finance_conn")
    response = requests.get(url, headers=api_conn.extra_dejson["headers"])
    response.raise_for_status()
    return json.dumps(response.json()["chart"]["result"][0])

def _store_stock_prices(stock_data) -> None:

    client = _get_minio_client()
    BUCKET_NAME = "stock-market"
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock_data)
    print(f"Store Stock Prices: {stock}")
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )
    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path: str):
    minio_client = _get_minio_client()

    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = minio_client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    return AirflowNotFoundException("File not found")