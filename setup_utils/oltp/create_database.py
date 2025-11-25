import logging

from sqlalchemy import create_engine
from sqlalchemy import text


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def create_database_postgres(
    db_name: str | None = None,
    host: str = "localhost",
    user: str = "postgres",
    password: str = "postgres",
    port: int = 5432,
) -> None:
    """
    Создаёт базу данных в PostgreSQL.

    :param db_name: Имя базы данных.
    :param host: Хост базы данных.
    :param user: Пользователь базы данных.
    :param password: Пароль пользователя базы данных.
    :param port: Порт базы данных.
    :return: None
    """

    # Подключаемся к БД postgres (системная БД)
    engine = create_engine(url=f"postgresql://{user}:{password}@{host}:{port}/postgres", isolation_level="AUTOCOMMIT")

    # Выполняем CREATE DATABASE
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {db_name}"))
        logging.info(f"✅ База данных '{db_name}' успешно создана")

    engine.dispose()


# Пример использования
if __name__ == "__main__":
    pass
