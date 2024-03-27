from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class FilmWork:
    id: str
    title: str
    description: Optional[str]  # Optional because it's nullable in the table definition
    creation_date: Optional[
        date
    ]  # Optional because it's nullable in the table definition
    file_path: Optional[str]  # Optional because it's nullable in the table definition
    rating: Optional[float]  # Optional because it's nullable in the table definition
    type: str
    created: Optional[str]  # Optional because it's nullable in the table definition
    modified: Optional[str]


@dataclass
class Genre:
    id: str
    name: str
    description: str
    created: Optional[str]  # Optional because it's nullable in the table definition
    modified: Optional[str]


@dataclass
class Person:
    id: str
    full_name: str
    created: str
    modified: str


@dataclass
class GenreFilmWork:
    id: str
    film_work_id: str
    genre_id: str
    created: str


@dataclass
class PersonFilmWork:
    id: str
    film_work_id: str
    person_id: str
    role: str
    created: str


sqlite_dataclass_mapping = {
    "film_work": FilmWork,
    "genre": Genre,
    "genre_film_work": GenreFilmWork,
    "person": Person,
    "person_film_work": PersonFilmWork,
}


def find_table_name(table_dataclass_mapping, data_type):
    """
    Find corresponding table name for the given dataclass type
    """
    for table_name, dataclass_type in table_dataclass_mapping.items():
        if dataclass_type == data_type:
            return table_name
    return None
