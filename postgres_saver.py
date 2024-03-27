from dataclasses import dataclass, astuple
from typing import Generator
from sqlite_models import table_dataclass_mapping, find_table_name


class PostgresSaver:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save_all_data(self, data: Generator[dataclass, None, None]):
        try:
            with self.pg_conn.cursor() as cursor:
                current_table_name = None
                batch = []
                for idx, item in enumerate(data, start=1):
                    data_type = type(item)
                    table_name = find_table_name(table_dataclass_mapping, data_type)
                    if table_name != current_table_name or len(batch) >= 50:
                        self.insert_batch(cursor, current_table_name, batch)
                        batch = []
                        current_table_name = table_name
                    batch.append(item)

                # Insert remaining records if any
                if batch:
                    self.insert_batch(cursor, current_table_name, batch)

                self.pg_conn.commit()
        except Exception as e:
            print(f"Error: {e}")

    def insert_batch(self, cursor, table_name, batch):
        if not batch:
            return

        column_names = [
            field.name for field in batch[0].__class__.__dataclass_fields__.values()
        ]
        column_names_str = ",".join(column_names)
        col_count = ",".join(["%s"] * len(column_names))

        values = [astuple(item) for item in batch]
        mogrified_values = [
            cursor.mogrify(f"({col_count})", value).decode("utf-8") for value in values
        ]

        query = (
            f"INSERT INTO content.{table_name} ({column_names_str}) VALUES "
            f'{",".join(mogrified_values)} '
            f"ON CONFLICT (id) DO NOTHING"
        )
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"Error: {e}")
            print(f"Query: {query}")
