import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path


class SQLiteExtractor:
    def __init__(self, db_path: Path):
    # def __init__(self, connection: sqlite3.Connection):
        self.db_path = db_path

    @contextmanager
    def conn_context(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        # conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def get_row_count_01(self, table_name):
        with self.conn_context() as conn:
            with closing(conn.cursor()) as cursor:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                except sqlite3.Error as e:
                    print(f"Error: {e}")
                    count = None
        return count

    def get_row_count_02(self, table_name):
        with self.conn_context() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
            except sqlite3.Error as e:
                print(f"Error: {e}")
                count = None
            finally:
                cursor.close()
            return count


if __name__ == "__main__":
    # with closing(sqlite3.connect("../db.sqlite")) as sqlite_conn:
    #     sqlite_extractor = SQLiteExtractor(sqlite_conn)

    db_path = Path("../db.sqlite")
    sqlite_extractor =  SQLiteExtractor(db_path)
    print('try-except-finally     -> ', sqlite_extractor.get_row_count_01("film_work"))
    print('with self.conn_context -> ', sqlite_extractor.get_row_count_02("film_work"))
