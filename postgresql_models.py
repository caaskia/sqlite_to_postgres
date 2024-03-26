from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional
import uuid

@dataclass
class Genre:
    id: uuid.UUID
    created: datetime
    modified: datetime
    name: str
    description: Optional[str]

@dataclass
class Person:
    id: uuid.UUID
    created: datetime
    modified: datetime
    full_name: str

@dataclass
class FilmWork:
    id: uuid.UUID
    created: datetime
    modified: datetime
    title: str
    description: Optional[str]
    creation_date: Optional[date]
    rating: Optional[float]
    type: str
    certificate: Optional[str]
    file_path: Optional[str]

@dataclass
class GenreFilmWork:
    id: uuid.UUID
    created: datetime
    film_work_id: uuid.UUID
    genre_id: uuid.UUID

@dataclass
class PersonFilmWork:
    id: uuid.UUID
    role: str
    created: datetime
    film_work_id: uuid.UUID
    person_id: uuid.UUID

postgresql_data_mapping = {
    'film_work': FilmWork,
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    'person': Person,
    'person_film_work': PersonFilmWork
}

