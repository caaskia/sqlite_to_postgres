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


class SQLiteExtractor:
    def __init__(self, connection):
        self.connection = connection

    def extract_film_works(self):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT id, title, description, creation_date, file_path, rating, type, created_at, updated_at FROM film_work")
        data = cursor.fetchall()
        cursor.close()
        films = [FilmWork(*row) for row in data]
        return films

    def extract_genres(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, name, description, created_at, updated_at FROM genre")
        data = cursor.fetchall()
        cursor.close()
        genres = [Genre(*row) for row in data]
        return genres

    def extract_genre_film_work(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, film_work_id, genre_id, created_at FROM genre_film_work")
        data = cursor.fetchall()
        cursor.close()
        genre_film_works = [GenreFilmWork(*row) for row in data]
        return genre_film_works

    def extract_persons(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, full_name, created_at, updated_at FROM person")
        data = cursor.fetchall()
        cursor.close()
        persons = [Person(*row) for row in data]
        return persons

    def extract_person_film_work(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, film_work_id, person_id, role, created_at FROM person_film_work")
        data = cursor.fetchall()
        cursor.close()
        person_film_works = [PersonFilmWork(*row) for row in data]
        return person_film_works


if __name__ == '__main__':
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)

        films_data = sqlite_extractor.extract_film_works()
        genres_data = sqlite_extractor.extract_genres()
        genre_film_works_data = sqlite_extractor.extract_genre_film_work()
        persons_data = sqlite_extractor.extract_persons()
        person_film_works_data = sqlite_extractor.extract_person_film_work()

        # Example usage
        print("Films:")
        for film in films_data:
            print(film)

        print("\nGenres:")
        for genre in genres_data:
            print(genre)

        print("\nGenre Film Works:")
        for gfw in genre_film_works_data:
            print(gfw)

        print("\nPersons:")
        for person in persons_data:
            print(person)

        print("\nPerson Film Works:")
        for pfw in person_film_works_data:
            print(pfw)
