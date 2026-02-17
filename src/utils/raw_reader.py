from sqlalchemy import create_engine, text
import pandas as pd


class RawLakeReader:
    """
    Reader READ-ONLY pour le lake tabulaire (schema RAW).
    """

    def __init__(self, pg_conn_str: str, schema: str = "raw"):
        self.engine = create_engine(pg_conn_str)
        self.schema = schema

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Lit une table RAW complète.
        """
        query = f"SELECT * FROM {self.schema}.{table_name}"
        return pd.read_sql(query, self.engine)

    def read_query(self, sql: str) -> pd.DataFrame:
        """
        Exécute une requête SQL libre sur le schema RAW.
        """
        return pd.read_sql(text(sql), self.engine)

    def list_tables(self) -> list[str]:
        """
        Liste les tables disponibles dans le schema RAW.
        """
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"schema": self.schema})
            return [row[0] for row in result]
