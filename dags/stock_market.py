from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata


from include.stock_market.tasks import _get_stock_prices, _store_stock_prices, _get_formatted_csv, BUCKET_NAME

SYMBOL = "TLSA"

@dag(
    start_date=datetime(2025, 4, 23),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=SlackNotifier(
        slack_conn_id="slack_conn",
        text="The DAG stock_market has completed.",
        channel="general"
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id="slack_conn",
        text="The DAG stock_market has failed.",
        channel="general"
    )
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

    format_prices_task = DockerOperator(
        task_id='format_prices',
        image="airflow/stock-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove="success",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ ti.xcom_pull(task_ids="store_prices") }}',
        }
    )

    get_formatted_csv_task = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='store_prices') }}",
        }
    )

    load_to_dw_task = aql.load_file(
        task_id="load_to_dw",
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id="minio_conn"
        ),
        output_table=Table(
            conn_id="postgres",
            name="stock_market",
            metadata=Metadata(schema="public")
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection("minio_conn").login,
            "aws_secret_access_key": BaseHook.get_connection("minio_conn").password,
            "endpoint_url": BaseHook.get_connection("minio_conn").host,
        }
    )

    is_api_available_task >> get_stock_prices_task >> store_prices_task >> format_prices_task >> get_formatted_csv_task >> load_to_dw_task

stock_market = stock_market()