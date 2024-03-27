import pytest
import sqlite3
import psycopg2
from sqlite_utils import SQLiteExtractor
from postgres_utils import PostgresExtractor
from datetime import datetime, timezone

from config import dsl

table_names = [
    "genre",
    "person",
    "film_work",
    "genre_film_work",
    "person_film_work",
]


@pytest.fixture(scope="module")
def sqlite_conn():
    conn = sqlite3.connect("../db.sqlite")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pg_conn():
    conn = psycopg2.connect(**dsl)
    yield conn
    conn.close()


def test_data_integrity(sqlite_conn, pg_conn):
    """Check data integrity between SQLite and PostgreSQL"""

    with sqlite_conn, pg_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        postgres_extractor = PostgresExtractor(pg_conn)

        for table_name in table_names:
            # Count rows
            # Получение количества записей из SQLite
            sqlite_count = sqlite_extractor.get_row_count(table_name)

            # Получение количества записей из PostgreSQL
            postgres_count = postgres_extractor.get_row_count(table_name)

            # Проверка целостности данных между таблицами
            assert sqlite_count == postgres_count, (
                f"Integrity check failed for table {table_name}. "
                f"SQLite count: {sqlite_count}, PostgreSQL count: {postgres_count}"
            )


def test_check_contents(sqlite_conn, pg_conn):
    """Check contents of records in each table"""

    with sqlite_conn, pg_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        postgres_extractor = PostgresExtractor(pg_conn)

        for table_name in table_names:
            # Get data from SQLite and PostgreSQL
            sqlite_data = sqlite_extractor.extract_data(table_name)

            # Проверка содержимого записей внутри каждой таблицы.
            # Проверrка, что все записи из PostgreSQL присутствуют с такими же значениями полей, как и в SQLite.
            for sqlite_record in sqlite_data:
                postgres_record = postgres_extractor.get_record_by_id(
                    table_name, sqlite_record.id
                )

                # Создаем словарь для хранения значений sqlite_record
                sqlite_values = {}

                # Обходим поля sqlite_record
                for name_field, value_field in sqlite_record.__dict__.items():
                    # Если поле является 'created', 'modified' и имеет тип str, то преобразуем его в datetime
                    if name_field in ("created", "modified") and isinstance(
                        value_field, str
                    ):
                        try:
                            # Используем регулярное выражение для разбора временной зоны
                            dt_str, tz_str = value_field.rsplit("+", 1)
                            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                            tz = (
                                timezone.utc
                                if tz_str == "00"
                                else timezone(
                                    datetime.timedelta(
                                        hours=int(tz_str[:2]), minutes=int(tz_str[2:])
                                    )
                                )
                            )
                            value_field = dt.replace(tzinfo=tz)
                        except Exception as e:
                            print(f"Ошибка при разборе временной зоны: {e}")
                            exit(1)
                    # Добавляем значение в словарь sqlite_values
                    sqlite_values[name_field] = value_field

                for name_field, value_field in sqlite_values.items():
                    # Проверяем, существует ли соответствующее поле в postgres_record и сравниваем значения
                    if name_field in postgres_record.__dict__:
                        assert value_field == getattr(
                            postgres_record, name_field
                        ), f"Значения поля {name_field} не совпадают."
