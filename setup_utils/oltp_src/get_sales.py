import datetime
import random

import pandas as pd


# noinspection PyTypeHints
def get_df_sales(
    df_users: pd.DataFrame | None = None,
    min_sales_per_user: int = 0,
    max_sales_per_user: int = 1000,
) -> pd.DataFrame:
    """
    Генерация датафрейма с продажами на основе датафрейма пользователей.

    :param df_users: Датафрейм с пользователями (должен содержать колонки 'id' и 'created_at').
    :param min_sales_per_user: Минимальное количество продаж на пользователя.
    :param max_sales_per_user: Максимальное количество продаж на пользователя.
    :return: Датафрейм с продажами.
    """
    list_of_sales = []
    sale_id = 1

    # Определяем конечную дату для генерации продаж (например, текущая дата)
    end_date = datetime.datetime(year=2025, month=10, day=13, tzinfo=datetime.UTC)

    for _, user in df_users.iterrows():
        user_id = user["id"]
        user_created_at = user["created_at"]

        user_created_at = pd.to_datetime(arg=user_created_at, utc=True)

        # Случайное количество продаж для этого пользователя
        num_sales = random.randint(a=min_sales_per_user, b=max_sales_per_user)  # noqa: S311

        for _ in range(num_sales):
            # Генерируем случайную дату продажи после регистрации пользователя
            time_delta = end_date - user_created_at

            random_seconds = random.randint(a=0, b=int(time_delta.total_seconds()))  # noqa: S311
            sale_created_at = user_created_at + datetime.timedelta(seconds=random_seconds)

            # Случайный филиал от 1 до 150
            branch = random.randint(a=1, b=150)  # noqa: S311

            amount = random.randint(a=999, b=99_900)  # noqa: S311

            sale_dict = {
                "id": sale_id,
                "user_id": user_id,
                "created_at": sale_created_at,
                "branch": branch,
                "amount": amount,
            }

            list_of_sales.append(sale_dict)
            sale_id += 1

    return pd.DataFrame(list_of_sales)


if __name__ == "__main__":
    pass
    # Пример использования
    # from get_users import get_df_users

    # Генерируем пользователей (для примера используем меньший размер)
    # df_users = get_df_users(size=101)  # 100 пользователей

    # Генерируем продажи
    # df_sales = get_df_sales(df_users=df_users)

    # df_sales.to_csv("sales.csv", index=False)
