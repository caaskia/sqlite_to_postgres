from config import dsl

import psycopg2
from psycopg2.extras import DictCursor

from postgres_utils import PostgresExtractor

if __name__ == '__main__':
    with (psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn):
        table_names = ['film_work', 'genre', 'person', 'genre_film_work', 'person_film_work']
        postgres_extractor = PostgresExtractor(pg_conn)
        for table_name in table_names:
            print(f"Table: {table_name}")
            for row in postgres_extractor.extract_data(table_name):
                print(row)
            print()
