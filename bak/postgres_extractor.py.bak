from config import dsl

import psycopg2
from psycopg2.extras import DictCursor
from dataclasses import dataclass
from typing import Generator, Type

from postgresql_models import postgresql_data_mapping


class PostgresExtractor:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def get_row_count(self, table_name):
        '''Get row count from table'''
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM content.{table_name}")
            count = cursor.fetchone()[0]
        return count

    def get_record_by_id(self, table_name, id):
        '''Get record by id from table'''
        # check table_name in mapping
        if table_name not in postgresql_data_mapping:
            raise ValueError(f"Table '{table_name}' not found in mapping")

        postgresql_data_class = postgresql_data_mapping[table_name]
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM content.{table_name} WHERE id = %s", (id,))
            record = cursor.fetchone()
            data_record = postgresql_data_class(*record)
        return data_record

    def extract_data(self, table_name: str) -> Generator[Type[dataclass], None, None]:
        '''Extract data from table'''
        if table_name not in postgresql_data_mapping:
            raise ValueError(f"Table '{table_name}' not found in mapping")

        dataclass_for_table = postgresql_data_mapping[table_name]

        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM content.{table_name}")

            part_size = 100
            while True:
                resp = cursor.fetchmany(part_size)
                if not resp:
                    break
                for row in resp:
                    yield dataclass_for_table(*row)




if __name__ == "__main__":
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        table_names = [
            "genre",
            "person",
            "genre_film_work",
            "person_film_work",
            "film_work",
        ]
        postgres_extractor = PostgresExtractor(pg_conn)
        for table_name in table_names:
            print(f"Table: {table_name}")
            for row in postgres_extractor.extract_data(table_name):
                print(row)
            print()
