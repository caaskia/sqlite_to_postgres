from dataclasses import dataclass, astuple, fields
from typing import Generator, Type
from models import table_dataclass_mapping

class PostgresSaver:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save_all_data(self, data: Generator[Type[dataclass], None, None]):
        # for table_name in table_dataclass_mapping.keys():
        #     self.save_data(data, table_name)
        try:
            with self.pg_conn as conn:
                with conn.cursor() as cursor:
                    column_names = [field.name for field in fields(data[0])]
                    column_names_str = ','.join(column_names)

                    col_count = ','.join(['%s'] * len(column_names))
                    bind_values = ','.join(cursor.mogrify(
                                        f"({col_count})",
                                        astuple(item)).decode('utf-8') for item in data
                                        )

                    query = (
                        f'INSERT INTO {table_name} ({column_names_str}) VALUES {bind_values} '
                        f' ON CONFLICT (id) DO NOTHING'
                    )
                    cursor.execute(query)
                    conn.commit()
        except Exception as e:
            print(f"Error: {e}")
