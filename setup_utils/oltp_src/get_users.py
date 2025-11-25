import datetime

import pandas as pd

from faker import Faker


def get_df_users(size: int = 10_001) -> pd.DataFrame:
    """
    Генерация датафрейма с пользователями.

    :param size: Размер датасета.
    :return: Датафрейм с пользователями.
    """
    fake = Faker(locale="ru_RU")

    list_of_dict = []
    for i in range(1, size):
        dict_ = {
            "id": i,
            "created_at": fake.date_time_ad(
                start_datetime=datetime.date(year=2024, month=1, day=1),
                end_datetime=datetime.date(year=2025, month=1, day=1),
            ),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "middle_name": fake.middle_name(),
            "birthday": fake.date_time_ad(
                start_datetime=datetime.date(year=1980, month=1, day=1),
                end_datetime=datetime.date(year=2005, month=1, day=1),
            ),
            "email": fake.email(),
        }

        list_of_dict.append(dict_)

    return pd.DataFrame(list_of_dict)


if __name__ == "__main__":
    pass
