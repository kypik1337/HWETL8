import datetime
import os
import requests
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    Float,
    bindparam,
)
from tabulate import tabulate


os.environ["no_proxy"] = "*"

CONNECTION_STRING = "mysql://root:1@localhost/spark"
ENGINE = None
METADATA = None


def _save_wether(table_name, temp):
    global ENGINE
    global METADATA

    if ENGINE is None:
        ENGINE = create_engine(CONNECTION_STRING)

    if METADATA is None:
        METADATA = MetaData()

    weather_tbl = Table(
        table_name,
        METADATA,
        Column("temperature", Float),
        Column("timestamp", DateTime),
    )
    METADATA.create_all(bind=ENGINE)

    stmt_insert = weather_tbl.insert().values(
        temperature=bindparam("temp"),
        timestamp=bindparam("time", type_=DateTime),
    )

    with ENGINE.connect() as conn:
        conn.execute(stmt_insert, {"temp": temp, "time": datetime.datetime.now()})


@dag(
    dag_id="wether-tlegram",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def WetherETL():
    send_message_telegram_task = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_default",
        token="6696218500:AAHJDQl2s2cpPPXxfx8gLVwl1SV4",
        chat_id="651217905",
        text="Wether in Moscow"
        + "\n```code"
        + "\n{{ ti.xcom_pull(task_ids=['generate_telegram_wether_table'], key='wether_table')[0] }}"
        + "\n```",
        telegram_kwargs={
            "parse_mode": "Markdown",
        },
    )

    @task(task_id="yandex_wether")
    def get_yandex_wether(**kwargs):
        ti = kwargs["ti"]
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"

        payload = {}
        headers = {"X-Yandex-API-Key": "33f45b91-bcd4-46e4-adc2-33cfdbbdd88e"}
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a = response.json()["fact"]["temp"]
        print(a)
        ti.xcom_push(key="wether", value=response.json()["fact"]["temp"])

    @task(task_id="save_yandex_wether")
    def save_yandex_wether(**kwargs):
        ti = kwargs["ti"]
        temp = ti.xcom_pull(task_ids=["yandex_wether"], key="wether")[0]
        print(f"Yandex temperature now is {temp}")
        _save_wether("yandex_wether", temp)

    #        return str(a)
    @task(task_id="open_wether")
    def get_open_wether(**kwargs):
        ti = kwargs["ti"]
        url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd78e55c423fc81cebc1487134a6300"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a = round(float(response.json()["main"]["temp"]) - 273.15, 2)
        print(a)
        ti.xcom_push(
            key="open_wether",
            value=round(float(response.json()["main"]["temp"]) - 273.15, 2),
        )

    @task(task_id="save_open_wether")
    def save_open_wether(**kwargs):
        ti = kwargs["ti"]
        temp = ti.xcom_pull(task_ids=["open_wether"], key="open_wether")[0]
        print(f"Temperature now is {temp}")
        _save_wether("open_wether", temp)

    #        return str(a)
    @task(task_id="python_wether")
    def get_wether(**kwargs):
        print(
            "Yandex "
            + str(kwargs["ti"].xcom_pull(task_ids=["yandex_wether"], key="wether")[0])
            + " Open "
            + str(
                kwargs["ti"].xcom_pull(task_ids=["open_wether"], key="open_wether")[0]
            )
        )

    @task(task_id="generate_telegram_wether_table")
    def generate_telegram_wether_table(**kwargs):
        ti = kwargs["ti"]

        ya_wether = ti.xcom_pull(task_ids=["yandex_wether"], key="wether")[0]
        op_wether = ti.xcom_pull(task_ids=["open_wether"], key="open_wether")[0]

        tbl = tabulate(
            (("Yandex", ya_wether), ("Open Wether", op_wether)),
            headers=["Source", "Degrees"],
            showindex=range(1, 3),
            tablefmt="github",
        )

        ti.xcom_push(
            key="wether_table",
            value=tbl,
        )

    @task(task_id="generate_payment_table")
    def generate_payment_table(**kwargs):
        ti = kwargs["ti"]

        global ENGINE
        global METADATA

        if ENGINE is None:
            ENGINE = create_engine(CONNECTION_STRING)

        df = pd.read_sql_query("select * from tasketl4b LIMIT 20", ENGINE)

        ti.xcom_push(
            key="payment_table",
            value=df.to_markdown(index=False),
        )

    send_payment_message_telegram_task = TelegramOperator(
        task_id="send_payment_message_telegram",
        telegram_conn_id="telegram_default",
        token="6696218500:AAHJDQl2s2cpPPXxfx8gLVwl1SV4",
        chat_id="651217905",
        text="```code"
        + "\n{{ ti.xcom_pull(task_ids=['generate_payment_table'], key='payment_table')[0] }}"
        + "\n```",
        telegram_kwargs={
            "parse_mode": "Markdown",
        },
    )

    (
        get_yandex_wether()
        >> save_yandex_wether()
        >> get_open_wether()
        >> save_open_wether()
        >> get_wether()
        >> generate_telegram_wether_table()
        >> send_message_telegram_task,
        generate_payment_table() >> send_payment_message_telegram_task,
    )


dag = WetherETL()
