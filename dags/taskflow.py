from airflow.decorators import dag, task
from datetime import datetime
from random import randint

@dag(
    start_date=datetime(2025, 4, 23),
    schedule_interval='@daily',
    catchup=False,
    tags=['task_flow'],
)
def task_flow():
    @task
    def get_number():
        n = randint(1, 100)
        print(f"Task A: {n}")
        return n

    @task
    def is_odd_or_even(value):
        result = "even" if value % 2 == 0 else "odd"
        print(f"Task B: {value} is {result}")

    is_odd_or_even(get_number())

task_flow = task_flow()