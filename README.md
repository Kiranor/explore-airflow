# Что такое Обратный ETL / Reverse ETL

Типовая задача дата-инженера – перенос данных из OLTP систем в OLAP системы.

Также у дата-инженера может возникнуть задача в виде Reverse ETL.

Суть Reverse ETL в том чтобы вернуть данные в OLTP-системы для работы продукта.

### Настройка окружения для корректной работы Airflow

Airflow 2.10.4 у нас с Python 3.12.8 (`>=3.12.8,<3.13`), поэтому виртуальное окружение необходимо создавать от Python3.12:

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

### Настройка Airflow через Docker

Мы используем Airflow, который собирается при помощи [Dockerfile](Dockerfile)
и [docker-compose.yaml](docker-compose.yaml).

Для запуска контейнера с Airflow, выполните команду:

```bash
docker-compose up -d
```

Веб-сервер Airflow запустится на хосте http://localhost:8080/, если не будет работать данный хост, то необходимо перейти
по хосту http://0.0.0.0:8080/.

#### Добавление пакетов в текущую сборку

Для того чтобы добавить какой-то пакет в текущую сборку, необходимо выполнить следующие шаги:

* Добавить новую строку в [Dockerfile](Dockerfile)
* Выполнить команду:

```bash
docker-compose build
```

* Выполнить команду:

```bash
docker-compose up -d
```

### Генерация данных для аналитики и проекта

Необходимо запустить:

1) [main.py](setup_utils/clickhouse/main.py) из папки [clickhouse](setup_utils/clickhouse)
2) [main.py](setup_utils/oltp_source/main.py) из папки [oltp_source](setup_utils/oltp_source)
3) [main.py](setup_utils/oltp_target/main.py) из папки [oltp_target](setup_utils/oltp_target)
4) [main.py](setup_utils/airflow/main.py) из папки [airflow](setup_utils/airflow)

Логика расчёта ранга в таблице `dds.user_ltv_history`:

| Класс | Лимит, ₽   | Скидка | Комментарий                               |
|-------|------------|--------|-------------------------------------------|
| A     | 0          | 0%     | Новички                                   |
| B     | 1 000 000  | 5%     | Первая ступень, достижима за пару месяцев |
| C     | 5 000 000  | 10%    | Уже активные                              |
| E     | 15 000 000 | 15%    | Примерно треть среднего LTV               |
| F     | 30 000 000 | 20%    | Около половины среднего LTV               |
| S     | 60 000 000 | 25%    | Ближе к среднему LTV                      |
| SS    | 90 000 000 | 30%    | Топ-10% или топ-5% пользователей          |

### Порядок запуска DAG

Запустить в следующей последовательности:

1) [users_to_ch.py](dags/users_to_ch.py)
2) [sales_to_ch.py](dags/sales_to_ch.py)
3) [sales_ltv_rank_from_ch_to_ch.py](dags/sales_ltv_rank_from_ch_to_ch.py)
4) [ltv_from_ch_to_pg.py](dags/ltv_from_ch_to_pg.py)