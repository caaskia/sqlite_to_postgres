from dataclasses import dataclass
from typing import Generator, Type, Tuple
import sqlite3

from sqlite_models import sqlite_dataclass_mapping, find_table_name
from config import table_names


class SQLiteExtractor:
    '''Class for extracting data from SQLite'''
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        # Так как имеет значение порядок таблиц (ForeignKey), то используем table_names из config.py
        # при полной автоматизации мы можем использовать self.table_list = self.get_table_list()
        self.table_list = table_names

    def get_table_list(self) -> list:
        '''Get list of tables from SQLite'''
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = cursor.fetchall()
        return [name[0] for name in table_names]

    def extract_data(self, table_name: str) -> Generator[Type[dataclass], None, None]:
        '''Extract data from table'''
        if table_name not in sqlite_dataclass_mapping:
            raise ValueError(f"Table '{table_name}' not found in mapping")

        dataclass_for_table = sqlite_dataclass_mapping[table_name]

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")

            part_size = 100
            while True:
                resp = cursor.fetchmany(part_size)
                if not resp:
                    break
                for row in resp:
                    yield dataclass_for_table(*row)


    def extract_movies(self) -> Generator[Type[dataclass], None, None]:
        '''Extract movies from all tables'''
        for table_name in self.table_list:
            print(f"Table: {table_name}")
            for row in self.extract_data(table_name):
                yield row

    def get_row_count(self, table_name):
        '''Get row count from table'''
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
        return count

    def extract_row_data(self, table_name: str) -> Generator[Tuple, None, None]:
        '''Extract row data from table'''
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield row


if __name__ == "__main__":
    with sqlite3.connect("db.sqlite") as sqlite_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)

        last_table_name = None
        for movie in sqlite_extractor.extract_movies():
            # Extract type from movie object
            movie_type = type(movie)
            # Find table name for the given dataclass type
            table_name = find_table_name(sqlite_dataclass_mapping, movie_type)
            if last_table_name != table_name:
                print(f"Data: {movie}")
            last_table_name = table_name
