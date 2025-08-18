# Импорт всех необходимых библиотек

from datetime import datetime, timedelta
import json
import requests
import logging
import os
import configparser

from pgdb import PGDatabase

config = configparser.ConfigParser()
dirname = os.path.dirname(__file__)
config.read(os.path.join(dirname, "config.ini"))

DATABASE_CREDS = config["Database"]
API_CREDS = config["Api"]

# Если папки с логами не существует - создаём её

logs_dir = os.path.join(dirname, "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

log_name = os.path.join(logs_dir, f"{datetime.now():%Y_%m_%d}.log")

# Удаление файлов с логами, созданных более чем 3 дня назад

for file in os.listdir(logs_dir):
    if file.endswith(".log"):
        if (
            datetime.now() - datetime.strptime(file.split(".")[0], "%Y_%m_%d")
        ).days > 3:
            os.remove(os.path.join(logs_dir, file))

# Настройка логирования

logging.basicConfig(
    filename=log_name,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Подключение к базе данных и создание новой таблицы, если она не существует

database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

creating_query = """
CREATE TABLE IF NOT EXISTS purchases (
        id SERIAL PRIMARY KEY,
        client_id INTEGER,
        gender TEXT,
        purchase_datetime DATE,
        purchase_time_as_seconds_from_midnight INTEGER,
        product_id INTEGER,
        quantity NUMERIC,
        price_per_item NUMERIC,
        discount_per_item NUMERIC,
        total_price NUMERIC
    );
"""
database.post(creating_query)

# Данные для подключения по API

api_url = API_CREDS["URL"]
headers = {"Accept": API_CREDS["ACCEPT"]}

date = datetime.now().date()

logging.info("Начало скачивания исторических данных по API")

while True:
    params = {"date": str(date)}

    columns = [
        "client_id",
        "gender",
        "purchase_datetime",
        "purchase_time_as_seconds_from_midnight",
        "product_id",
        "quantity",
        "price_per_item",
        "discount_per_item",
        "total_price",
    ]

    cols_str = ", ".join(columns)
    values_str = ", ".join(["%s"] * len(columns))
    try:
        r = requests.get(url=api_url, params=params, headers=headers)
        r.raise_for_status()
        purchases = r.json()

        if not purchases:
            logging.info(f"Данных за {date} нет. Останавливаем.")
            break

        logging.info(f"Скачано {len(purchases)} записей за {date}")

        query = f"""
            INSERT INTO purchases ({cols_str})
            VALUES ({values_str})
            ON CONFLICT DO NOTHING;
        """

        # Занесение загруженных данных в БД

        logging.info("Начало заполнения базы данных")

        try:
            for i, purchase in enumerate(purchases, start=1):
                values = [purchase[col] for col in columns]
                database.post(query, values)
            logging.info("Данные записаны в базу")
        except Exception as err:
            logging.error(f"Ошибка при записи в БД: {err}")
            try:
                if getattr(database, "connection", None):
                    database.connection.rollback()
            except Exception:
                logging.exception("Ошибка при rollback")

    except Exception as err:
        logging.error(f"Ошибка доступа к API за {date}: {err}")
        break

    date -= timedelta(days=1)

try:
    if getattr(database, "connection", None):
        database.connection.close()
except Exception:
    logging.exception("Ошибка при закрытии соединения")
logging.info("Соединение с базой данных закрыто")
