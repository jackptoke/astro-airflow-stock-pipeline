from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import json
def _get_stock_prices(url: str, symbol: str) -> dict:
    import requests

    url = f"{url}{symbol}?metrics=hight?&interval=1d&range=1y"
    api_conn = BaseHook.get_connection("yahoo_finance_conn")
    response = requests.get(url, headers=api_conn.extra_dejson["headers"])
    response.raise_for_status()
    return json.dumps(response.json()["chart"]["result"][0])

def _store_stock_prices(stock_data) -> None:
    minio_conn = BaseHook.get_connection("minio_conn")
    client = Minio(
        endpoint=minio_conn.extra_dejson["endpoint_url"].split('//')[1],
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False
    )
    bucket_name = "stock-market"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock_data)
    print(f"Store Stock Prices: {stock}")
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )
    return f'{objw.bucket_name}/{symbol}'