import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


OWNER = "n.grigorev"
DAG_ID = "sales_to_ch"

GP_TARGET_SCHEMA = "ods"
GP_TARGET_TABLE = "ods_sales"
GP_TMP_SCHEMA = "stg"

LONG_DESCRIPTION = """
# Загрузка данных о продажах в ClickHouse через временную таблицу
"""

SHORT_DESCRIPTION = "Загрузка данных о продажах в ClickHouse"

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
    tags=["oltp_sales", "ch"],
    description=SHORT_DESCRIPTION,
    concurrency=10,
    max_active_tasks=10,
    max_active_runs=10,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Имя временной таблицы
    tmp_table = f"tmp_{GP_TARGET_TABLE}_{{{{ data_interval_start.format('YYYYMMDD') }}}}"

    drop_tmp_table = ClickHouseOperator(
        task_id="drop_tmp_table",
        sql=f"DROP TABLE IF EXISTS {GP_TMP_SCHEMA}.{tmp_table}",
        clickhouse_conn_id="olap_ch",
    )

    create_tmp_table = ClickHouseOperator(
        task_id="create_tmp_table",
        sql=f"""
        CREATE TABLE {GP_TMP_SCHEMA}.{tmp_table}
        ENGINE = MergeTree
        ORDER BY tuple()
        AS
        SELECT * FROM et.et_ods_sales
        WHERE
            1=1
            AND created_at BETWEEN '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
            AND '{{{{ data_interval_end.format('YYYY-MM-DD') }}}}'
        """,
        clickhouse_conn_id="olap_ch",
    )

    delete_from_target = ClickHouseOperator(
        task_id="delete_from_target",
        sql=f"""
        ALTER TABLE {GP_TARGET_SCHEMA}.{GP_TARGET_TABLE}
        DELETE WHERE
            created_at BETWEEN '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
            AND '{{{{ data_interval_end.format('YYYY-MM-DD') }}}}'
        """,
        clickhouse_conn_id="olap_ch",
    )

    insert_into_target = ClickHouseOperator(
        task_id="insert_into_target",
        sql=f"""
        INSERT INTO {GP_TARGET_SCHEMA}.{GP_TARGET_TABLE}
        SELECT * FROM {GP_TMP_SCHEMA}.{tmp_table}
        """,
        clickhouse_conn_id="olap_ch",
    )

    drop_tmp_table_final = ClickHouseOperator(
        task_id="drop_tmp_table_final",
        sql=f"DROP TABLE IF EXISTS {GP_TMP_SCHEMA}.{tmp_table}",
        clickhouse_conn_id="olap_ch",
    )

    (
        start
        >> drop_tmp_table
        >> create_tmp_table
        >> delete_from_target
        >> insert_into_target
        >> drop_tmp_table_final
        >> end
    )
