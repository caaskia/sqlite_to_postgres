import psycopg2.extras

from uuid import UUID
from dataclasses import dataclass, asdict, astuple, fields

dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

@dataclass
class User:
    id: UUID
    name: str

# необходимо для работы с UUID https://www.psycopg.org/docs/extras.html#uuid-data-type
psycopg2.extras.register_uuid()

with psycopg2.connect(**dsn) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    ) as cursor:

    cursor.execute("""SELECT id, name FROM content.temp_table""")

    result = cursor.fetchone()

    user = User(**result) # User(id=UUID('b8531efb-c49d-4111-803f-725c3abc0f5e'), name='Василий Васильевич')

    asdict(user)  # {'id': UUID('b8531efb-c49d-4111-803f-725c3abc0f5e'), 'name': 'Василий Васильевич'}

    astuple(user)  # (UUID('b8531efb-c49d-4111-803f-725c3abc0f5e'), 'Василий Васильевич')

    [field.name for field in fields(user)]  # [id, name]

    user1 = User(UUID('393ade24-18a4-41cd-911e-39208bbffa74'), 'Петров Игнат')
    user2 = User(UUID('1f947417-ec96-44a3-97e1-bf39a26f4941'), 'Сидоров Ларион')

    users = [user1, user2]

    # Получаем названия колонок таблицы (полей датакласса)
    column_names = [field.name for field in fields(user1)]  # [id, name]
    column_names_str = ','.join(column_names)  # id, name

    # В зависимости от количества колонок генерируем под них %s.
    col_count = ', '.join(['%s'] * len(column_names))  # '%s, %s

    bind_values = ','.join(cursor.mogrify(f"({col_count})", astuple(user)).decode('utf-8') for user in users)

    query = (f'INSERT INTO content.temp_table ({column_names_str}) VALUES {bind_values} '
             f' ON CONFLICT (id) DO NOTHING'
             )
    cursor.execute(query)
