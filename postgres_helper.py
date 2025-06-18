import logging
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.sql.schema import MetaData, Table


class PostgresConnection:

    def __init__(self, conn_id: str = None):
        self.engine = create_engine(conn_id)
        self.session = sessionmaker(self.engine)

    def close(self):
        if self.session:
            self.session.close_all()

    def execute_and_return_dataframe(self, query):
        conn = self.engine.connect()
        cursor = conn.execute(text(query))
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
        conn.close()
        return df

    def execute_and_return_dict(self, query: str, columns: list) -> list:
        def row_to_dict(row) -> dict:
            result = {}
            for idx, val in enumerate(columns):
                result[val] = row[idx]
            return result

        conn = self.engine.connect()
        rows = conn.execute(text(query))
        if len(columns) > 0:
            return list(map(lambda x: row_to_dict(x), rows))
        return rows

    def bulk_upsert_for_dict(self, schema: str, table_name: str, primary_keys: list, items: list):
        meta = MetaData(schema=schema)
        conn = self.engine.connect()
        table = Table(table_name, meta, autoload_with=conn)
        self._bulk_upsert_for_dict(table=table, primary_keys=primary_keys, items=items)
        logging.info(f'bulk_upsert_for_dict for {len(items)} items')
        conn.close()

    def _bulk_upsert_for_dict(self, table, primary_keys, items):
        statement = postgresql.insert(table).values(items)
        if primary_keys:
            statement = statement.on_conflict_do_update(
                index_elements=primary_keys,
                set_={c.key: c for c in statement.excluded if c.key not in primary_keys + ['created_at']}
            )

        with self.session_scope() as session:
            session.execute(statement)

    @staticmethod
    def _to_dict(row):
        return dict(map(lambda x: (x.name, getattr(row, x.name)), row.__table__.columns))

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
