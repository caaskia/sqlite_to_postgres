import pytest
import sqlite3
import psycopg2
from load_data import load_from_sqlite
from sqlite_utils import SQLiteExtractor
from postgres_extractor import PostgresExtractor
from sqlite_models import *

from config import dsl


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
    load_from_sqlite(sqlite_conn, pg_conn)

    with sqlite_conn, pg_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        postgres_extractor = PostgresExtractor(pg_conn)

        table_names = [
            "genre",
            "person",
            "film_work",
            "genre_film_work",
            "person_film_work",
        ]

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

            # Get dataclass for the current table
            data_class = table_dataclass_mapping.get(table_name)
            if data_class is None:
                continue

            # Get data from SQLite and PostgreSQL
            sqlite_data = sqlite_extractor.extract_data(table_name)
            postgres_data = postgres_extractor.extract_data(table_name)

            # Check data
            # for sqlite_row, postgres_row in zip(sqlite_data, postgres_data):
            #     assert sqlite_row == postgres_row, f"Data mismatch for table {table_name}. " \
            #                                        f"SQLite row: {sqlite_row}, PostgreSQL row: {postgres_row}"

            # Assuming sqlite_row and postgres_row are instances of the Genre class
            for sqlite_row, postgres_row in zip(sqlite_data, postgres_data):
                # Create copies of the rows without the created and modified fields
                sqlite_row_without_timestamps = sqlite_row.__class__(
                    **{
                        k: v
                        for k, v in sqlite_row.__dict__.items()
                        if k not in ["created", "modified"]
                    }
                )
                postgres_row_without_timestamps = postgres_row.__class__(
                    **{
                        k: v
                        for k, v in postgres_row.__dict__.items()
                        if k not in ["created", "modified"]
                    }
                )

                # Now compare the rows without the created and modified fields
                assert (
                    sqlite_row_without_timestamps == postgres_row_without_timestamps
                ), (
                    f"Data mismatch for table {table_name}. "
                    f"SQLite row: {sqlite_row_without_timestamps}, "
                    f"PostgreSQL row: {postgres_row_without_timestamps}"
                )
