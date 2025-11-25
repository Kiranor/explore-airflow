import pandas as pd


# noinspection PyTypeHints
def df_to_postgresql(
    df: pd.DataFrame | None = None,
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "postgres",
    db: str = "postgres",
    table: str | None = None,
) -> None:
    """
    Загружает DataFrame в таблицу PostgreSQL.

    :param df: DataFrame для загрузки.
    :param host: Host базы данных.
    :param port: Port базы данных.
    :param user: User базы данных.
    :param password: Password базы данных.
    :param db: DB базы данных.
    :param table: Table базы данных.
    :return: Ничего не возвращает.
    """
    df.to_sql(
        name=table,
        con=f"postgresql://{user}:{password}@{host}:{port}/{db}",
        if_exists="append",
        chunksize=100_000,
        index=False,
    )


if __name__ == "__main__":
    pass
