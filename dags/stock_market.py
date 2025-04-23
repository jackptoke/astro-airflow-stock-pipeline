from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices, _store_stock_prices

SYMBOL = "TLSA"

@dag(
    start_date=datetime(2025, 4, 23),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        api = BaseHook.get_connection('yahoo_finance_conn')
        url = f"{api.host}{api.extra_dejson["endpoint"]}"
        print(f"URL: {url}")

        response = requests.get(f"{url}tlsa?metrics=hight?&interval=1d&range=1y", headers=api.extra_dejson["headers"])
        response.raise_for_status()
        data = response.json()
        condition = data["chart"]["result"] is not None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    is_api_available_task = is_api_available()

    get_stock_prices_task = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={ 'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},
    )

    store_prices_task = PythonOperator(
        task_id='store_prices',
        python_callable=_store_stock_prices,
        op_kwargs={'stock_data': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'},
    )

    is_api_available_task >> get_stock_prices_task
    get_stock_prices_task >> store_prices_task

stock_market = stock_market()