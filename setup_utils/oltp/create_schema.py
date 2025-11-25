import logging

from sqlalchemy import create_engine
from sqlalchemy import text


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def create_schema_postgres(
    db_name: str | None = "postgres",
    host: str = "localhost",
    user: str = "postgres",
    password: str = "postgres",
    port: int = 5432,
    schema: str = "stg",
) -> None:
    """
    Создаёт схему в базе данных PostgreSQL.

    :param db_name: Имя базы данных.
    :param host: Хост базы данных.
    :param user: Пользователь базы данных.
    :param password: Пароль пользователя базы данных.
    :param port: Порт базы данных.
    :param schema: Схема базы данных.
    :return: None
    """

    # Подключаемся к БД postgres (системная БД)
    engine = create_engine(url=f"postgresql://{user}:{password}@{host}:{port}/{db_name}", isolation_level="AUTOCOMMIT")

    # Выполняем CREATE DATABASE
    with engine.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
        logging.info(f"✅ Схема базы данных '{schema}' успешно создана")

    engine.dispose()


# Пример использования
if __name__ == "__main__":
    pass
