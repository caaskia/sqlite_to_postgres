import os
from dotenv import load_dotenv

load_dotenv()

dsl = {
    "dbname": os.environ.get('DB_NAME'),
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASSWORD'),
    "host": os.environ.get('DB_HOST', '127.0.0.1'),
    "port": os.environ.get('DB_PORT', 5432),
}

table_names = [
    "genre",
    "person",
    "film_work",
    "genre_film_work",
    "person_film_work",
]
