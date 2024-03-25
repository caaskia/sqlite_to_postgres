import sqlite3

from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn # С конструкцией yield вы познакомитесь в следующем модуле
                   # Пока воспринимайте её как return, после которого код может продолжить выполняться дальше
    # Даже если в процессе соединения произойдёт ошибка, блок finally всё равно его закроет
    finally:
        conn.close()


if __name__ == '__main__':
    db_path = 'db.sqlite'
    with conn_context(db_path) as conn:
        # По-умолчанию SQLite возвращает строки в виде кортежа значений.
        # Эта строка указывает, что данные должны быть в формате «ключ-значение»
        conn.row_factory = sqlite3.Row
        # Получаем курсор
        curs = conn.cursor()
        curs.execute("SELECT * FROM film_work;")
        # Получаем данные
        data = curs.fetchall()
        # Рассматриваем первую запись
        print(dict(data[0]))

    print('Connection is closed: ')
