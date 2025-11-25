import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from clickhouse_driver import Client


OWNER = "n.grigorev"
DAG_ID = "ltv_from_ch_to_pg"
LONG_DESCRIPTION = "Передача LTV-истории пользователей за execution_day в Postgres через внешнюю таблицу ClickHouse"
DAG_LIST_SENSOR = [
    "sales_ltv_rank_from_ch_to_ch",
]
TARGET_TABLE = "user_ltv_history"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2024, month=1, day=1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "depends_on_past": True,
}


def check_ch_data(**context) -> None:
    """
    Проверяет наличие данных в ClickHouse за execution_day.

    Если данных нет, то падает с ошибкой, чтобы запустить retry.
    :param context: Контекст DAG.
    :return: Ничего не возвращает.
    """

    ch_client = Client(
        host="olap_ch",
        user="click",
        password="click",  # noqa: S106
        database="dds",
    )
    execution_day = context["data_interval_start"].format("YYYY-MM-DD")
    result = ch_client.execute(
        f"SELECT count() FROM dds.user_ltv_history WHERE execution_day = '{execution_day}'",
    )
    logging.info(f"💻 Найдено {result[0][0]} строк за execution_day={execution_day} в ClickHouse")
    if result[0][0] == 0:
        raise ValueError(f"No data for execution_day={execution_day}! Will retry.")  # noqa: TRY003, EM102


with DAG(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    default_args=args,
    tags=["ltv", "clickhouse", "postgres", "integration"],
    description=LONG_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    sensors = [
        ExternalTaskSensor(
            task_id=f"sensor_{dag}",
            external_dag_id=dag,
            allowed_states=["success"],
            mode="reschedule",
            timeout=36000,  # длительность работы сенсора
            poke_interval=600,  # частота проверки
        )
        for dag in DAG_LIST_SENSOR
    ]

    # Имя временной таблицы (на каждый запуск)
    tmp_table = f"{TARGET_TABLE}_{{{{ data_interval_start.format('YYYYMMDD') }}}}"

    drop_tmp_table_before_in_target = PostgresOperator(
        task_id="drop_tmp_table_before_in_target",
        sql=f"DROP TABLE IF EXISTS stg.{tmp_table}",
        postgres_conn_id="oltp_target",
    )

    drop_et_tmp_table_before = ClickHouseOperator(
        task_id="drop_et_tmp_table_before",
        sql=f"DROP TABLE IF EXISTS et.{tmp_table}",
        clickhouse_conn_id="olap_ch",
    )

    check_data = PythonOperator(
        task_id="check_data",
        python_callable=check_ch_data,
        provide_context=True,
    )

    create_tmp_table_in_target = PostgresOperator(
        task_id="create_tmp_table_in_target",
        sql=f"""
            CREATE TABLE stg.{tmp_table}
            (
                user_id BIGINT,
                stat_date DATE,
                ltv BIGINT,
                user_class TEXT
            );
            """,
        postgres_conn_id="oltp_target",
    )

    create_et_tmp_table = ClickHouseOperator(
        task_id="create_et_tmp_table",
        sql=f"""
        CREATE TABLE et.{tmp_table}
        (
            user_id Int64,
            stat_date Date,
            ltv Int64,
            user_class String
        )
        ENGINE = PostgreSQL(
            'oltp_target:5432',
            'ltv',
            '{tmp_table}',
            'postgres',
            'postgres',
            'stg'
        );
        """,
        clickhouse_conn_id="olap_ch",
    )

    transfer_to_pg_temp = ClickHouseOperator(
        task_id="transfer_to_pg_temp",
        sql=f"""
        INSERT INTO et.{tmp_table} (user_id, stat_date, ltv, user_class)
        SELECT user_id, execution_day, ltv, user_class
        FROM dds.user_ltv_history
        WHERE execution_day = toDate('{{{{ data_interval_start.format('YYYY-MM-DD') }}}}')
        """,
        clickhouse_conn_id="olap_ch",
    )

    truncate_pg_main = PostgresOperator(
        task_id="truncate_pg_main",
        sql="TRUNCATE TABLE user_ltv_history;",
        postgres_conn_id="oltp_target",
    )

    insert_into_pg_main = PostgresOperator(
        task_id="insert_into_pg_main",
        sql=f"""
        INSERT INTO user_ltv_history (user_id, stat_date, ltv, user_class)
        SELECT user_id, stat_date, ltv, user_class FROM stg.{tmp_table};
        """,
        postgres_conn_id="oltp_target",
    )

    drop_tmp_table_after_in_target = PostgresOperator(
        task_id="drop_tmp_table_after_in_target",
        sql=f"DROP TABLE IF EXISTS stg.{tmp_table}",
        postgres_conn_id="oltp_target",
    )

    drop_et_tmp_table_after = ClickHouseOperator(
        task_id="drop_et_tmp_table_after",
        sql=f"DROP TABLE IF EXISTS et.{tmp_table}",
        clickhouse_conn_id="olap_ch",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> sensors
        >> drop_tmp_table_before_in_target
        >> drop_et_tmp_table_before
        >> check_data
        >> create_tmp_table_in_target
        >> create_et_tmp_table
        >> transfer_to_pg_temp
        >> truncate_pg_main
        >> insert_into_pg_main
        >> drop_tmp_table_after_in_target
        >> drop_et_tmp_table_after
        >> end
    )
