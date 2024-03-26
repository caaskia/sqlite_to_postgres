from dataclasses import dataclass
from typing import Generator, Type, Tuple
import sqlite3
from sqlite_models import table_dataclass_mapping, find_table_name

class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.table_list = self.get_table_list()

    def get_table_list(self) -> list:
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        cursor.close()
        return [name[0] for name in table_names]

    def extract_data(self, table_name: str) -> Generator[Type[dataclass], None, None]:
        if table_name not in table_dataclass_mapping:
            raise ValueError(f"Table '{table_name}' not found in mapping")

        dataclass_for_table = table_dataclass_mapping[table_name]

        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")

        part_size = 100
        while True:
            resp = cursor.fetchmany(part_size)
            if not resp:
                break
            for row in resp:
                yield dataclass_for_table(*row)

        cursor.close()

    def extract_movies(self) -> Generator[Type[dataclass], None, None]:
        for table_name in self.table_list:
            print(f"Table: {table_name}")
            for row in self.extract_data(table_name):
                yield row


    def get_row_count(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def extract_row_data(self, table_name: str) -> Generator[Tuple, None, None]:
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row
        cursor.close()


if __name__ == '__main__':
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)

        last_table_name = None
        for movie in sqlite_extractor.extract_movies():
            # Extract type from movie object
            movie_type = type(movie)
            # Find table name for the given dataclass type
            table_name = find_table_name(table_dataclass_mapping, movie_type)
            if last_table_name != table_name:
                # print(f"Table name: {table_name} - Data: {movie}")
                print(f"Data: {movie}")
            last_table_name = table_name
