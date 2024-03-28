from dataclasses import dataclass, astuple
from typing import Generator

from postgresql_models import postgresql_data_mapping
from sqlite_models import sqlite_dataclass_mapping, find_table_name

import logging
logging.basicConfig(level=logging.DEBUG, format="%(module)s - %(message)s")


class PostgresSaver:
    '''Class for saving data to PostgreSQL'''
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save_all_data(self, data: Generator[dataclass, None, None]):
        '''Save all data to PostgreSQL'''
        try:
            with self.pg_conn.cursor() as cursor:
                current_table_name = None
                batch = []
                for idx, item in enumerate(data, start=1):
                    data_type = type(item)
                    table_name = find_table_name(sqlite_dataclass_mapping, data_type)
                    if table_name != current_table_name or len(batch) >= 50:
                        self.insert_batch(cursor, current_table_name, batch)
                        batch = []
                        current_table_name = table_name
                    batch.append(item)

                # Insert remaining records if any
                if batch:
                    self.insert_batch(cursor, current_table_name, batch)

                self.pg_conn.commit()
        except Exception as e:
            logging.error(f"Error: {e}")

    def insert_batch(self, cursor, table_name, batch):
        '''Insert batch of data to PostgreSQL'''
        if not batch:
            return

        column_names = [
            field.name for field in batch[0].__class__.__dataclass_fields__.values()
        ]
        column_names_str = ",".join(column_names)
        col_count = ",".join(["%s"] * len(column_names))

        values = [astuple(item) for item in batch]
        mogrified_values = [
            cursor.mogrify(f"({col_count})", value).decode("utf-8") for value in values
        ]

        query = (
            f"INSERT INTO content.{table_name} ({column_names_str}) VALUES "
            f'{",".join(mogrified_values)} '
            f"ON CONFLICT (id) DO NOTHING"
        )
        try:
            cursor.execute(query)
        except Exception as e:
            logging.error(f"Error: {e}")
            logging.error(f"Query: {query}")

class PostgresExtractor:
    '''Class for extracting data from PostgreSQL'''
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
            # Retrieve the fields of the data class
            fields = [f.name for f in postgresql_data_class.__dataclass_fields__.values()]

            # Construct the SQL query string dynamically
            fields_str = ', '.join(fields)
            query = f"SELECT {fields_str} FROM content.{table_name} WHERE id = %s"

            # Execute the SQL query
            cursor.execute(query, (id,))

            record = cursor.fetchone()
            data_record = postgresql_data_class(*record)
        return data_record
