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

class SQLiteExtractor:
    def __init__(self, connection):
        self.connection = connection

    def extract_movies(self):
        """Extract data from the film_work table"""
        cursor = self.connection.cursor()

        # Execute a SELECT query to fetch data from the film_work table
        cursor.execute("SELECT id, title, description, creation_date, file_path, rating, type, created_at, updated_at FROM film_work")

        # Fetch all rows of data
        data = cursor.fetchall()

        # Close the cursor
        cursor.close()

        # Convert fetched data to FilmWork objects
        movies = [FilmWork(*row) for row in data]

        return movies

if __name__ == '__main__':
    # Example usage
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        movies_data = sqlite_extractor.extract_movies()
        for movie in movies_data:
            print(movie)
