import sqlite3

import psycopg2 # poetry add psycopg2-binary
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import dsl
from postgres_utils import PostgresSaver
from sqlite_utils import SQLiteExtractor


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""

    sqlite_extractor = SQLiteExtractor(connection)
    postgres_saver = PostgresSaver(pg_conn)

    data = sqlite_extractor.extract_movies()
    # for item in data:
    #     print(item)
    postgres_saver.save_all_data(data)


if __name__ == '__main__':

    with (sqlite3.connect('db.sqlite') as sqlite_conn,
          psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn):
        load_from_sqlite(sqlite_conn, pg_conn)
