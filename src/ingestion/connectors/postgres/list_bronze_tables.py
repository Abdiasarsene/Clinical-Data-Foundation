# src/ingestion/connectors/postgres/list_bronze_tables.py
from sqlalchemy import inspect
from src.ingestion.connectors.postgres.postgres import get_postgres_engine

BRONZE_SCHEMA = "bronze"


def list_bronze_tables() -> list[str]:
    """
    Retourne la liste des tables existantes dans le schema bronze
    """
    engine = get_postgres_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=BRONZE_SCHEMA)
    return tables