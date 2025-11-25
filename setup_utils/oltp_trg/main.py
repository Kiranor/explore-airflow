from setup_utils.oltp.create_database import create_database_postgres
from setup_utils.oltp.create_schema import create_schema_postgres
from setup_utils.oltp.execute_custom_query import execute_custom_query_postgres


if __name__ == "__main__":
    create_database_postgres(port=5434, db_name="ltv")
    create_schema_postgres(port=5434, db_name="ltv")
    execute_custom_query_postgres(
        db_name="ltv",
        port=5434,
        query="""
        CREATE TABLE user_ltv_history (
            user_id BIGINT PRIMARY KEY,
            stat_date DATE,
            ltv BIGINT,
            user_class TEXT
        );
        """,
    )
