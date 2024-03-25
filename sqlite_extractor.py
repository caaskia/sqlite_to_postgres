import sqlite3
from dataclasses import dataclass


@dataclass
class FilmWork:
    id: str
    title: str
    description: str
    creation_date: str
    file_path: str
    rating: float
    type: str
    created_at: str
    updated_at: str


@dataclass
class Genre:
    id: str
    name: str
    description: str
    created_at: str
    updated_at: str


@dataclass
class GenreFilmWork:
    id: str
    film_work_id: str
    genre_id: str
    created_at: str


@dataclass
class Person:
    id: str
    full_name: str
    created_at: str
    updated_at: str


@dataclass
class PersonFilmWork:
    id: str
    film_work_id: str
    person_id: str
    role: str
    created_at: str

table_dataclass_mapping = {
    'film_work': FilmWork,
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    'person': Person,
    'person_film_work': PersonFilmWork
}


class SQLiteExtractor:
    def __init__(self, connection):
        self.connection = connection
        self.table_list = self.get_table_list()

    def get_table_list(self):
        cursor = self.connection.cursor()
        # Query to retrieve all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        cursor.close()
        # Extracting table names from the result
        return [name[0] for name in table_names]

    def extract_data(self, table_name):
        dataclass_for_table = table_dataclass_mapping.get(table_name)

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


if __name__ == '__main__':
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)

        for table_name in sqlite_extractor.table_list:
            print(f"Table: {table_name}")
            for row in sqlite_extractor.extract_data(table_name):
                print(row)



