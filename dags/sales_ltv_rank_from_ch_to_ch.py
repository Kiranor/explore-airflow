import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


OWNER = "n.grigorev"
DAG_ID = "sales_ltv_rank_from_ch_to_ch"
LONG_DESCRIPTION = """
# Ежедневный расчёт LTV и класса пользователя по продажам, с сенсорами на загрузку данных
"""

SHORT_DESCRIPTION = "Ежедневный расчёт LTV и класса пользователя по продажам"

DAG_LIST_SENSOR = [
    "sales_to_ch",
    "users_to_ch",
]

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2024, month=1, day=1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "depends_on_past": True,
}

with DAG(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    default_args=args,
    tags=["ltv", "sales", "analytics", "ch"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

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

    calc_ltv_and_rank = ClickHouseOperator(
        task_id="calc_ltv_and_rank",
        sql="""
        INSERT INTO dds.user_ltv_history
        SELECT
            u.id AS user_id,
            '{{ data_interval_start.format('YYYY-MM-DD') }}' AS execution_day,
            coalesce(sum(s.amount), 0) AS ltv,
            CASE
                WHEN coalesce(sum(s.amount), 0) >= 90000000 THEN 'SS'
                WHEN coalesce(sum(s.amount), 0) >= 60000000 THEN 'S'
                WHEN coalesce(sum(s.amount), 0) >= 30000000 THEN 'F'
                WHEN coalesce(sum(s.amount), 0) >= 15000000 THEN 'E'
                WHEN coalesce(sum(s.amount), 0) >= 5000000  THEN 'C'
                WHEN coalesce(sum(s.amount), 0) >= 1000000  THEN 'B'
                ELSE 'A'
            END AS user_class
        FROM ods.ods_users u
        LEFT JOIN ods.ods_sales s
            ON u.id = s.user_id
            AND s.created_at < toDateTime('{{ data_interval_start.format('YYYY-MM-DD') }}')
        GROUP BY u.id
        """,
        clickhouse_conn_id="olap_ch",
    )

    end = EmptyOperator(task_id="end")

    (start >> sensors >> calc_ltv_and_rank >> end)
