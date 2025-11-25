import logging

from dataframe_to_pg import df_to_postgresql
from get_sales import get_df_sales
from get_users import get_df_users

from setup_utils.oltp.create_database import create_database_postgres


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

if __name__ == "__main__":
    create_database_postgres(db_name="oltp_users", port=5433)
    create_database_postgres(db_name="oltp_sales", port=5433)
    df_users = get_df_users()
    logging.info(f"👽 Количество пользователей: {len(df_users)}")

    df_sales = get_df_sales(df_users=df_users)
    logging.info(f"💰 Количество продаж: {len(df_sales)}")

    df_to_postgresql(df=df_users, port=5433, db="oltp_users", table="users")
    logging.info("💿 Данные пользователей успешно загружены в PostgreSQL – oltp_users, таблица – users")
    df_to_postgresql(df=df_sales, port=5433, db="oltp_sales", table="sales")
    logging.info("💿 Данные продаж успешно загружены в PostgreSQL – oltp_sales, таблица – sales")
