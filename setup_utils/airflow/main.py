from setup_utils.airflow.create_connection import create_airflow_connection

if __name__ == "__main__":
    create_airflow_connection(
        connection_id="olap_ch",
        conn_type="sqlite",
        host="olap_ch",
        port=9000,
        login="click",
        password="click",
        schema="default",
        user_name="airflow",
        password_auth="airflow",
    )

    create_airflow_connection(
        connection_id="oltp_target",
        schema="ltv",
        host="oltp_target",
        user_name="airflow",
        password_auth="airflow",
    )
