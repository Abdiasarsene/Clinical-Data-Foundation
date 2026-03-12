# src/ingestion/connectors/postgres/init_postgres.py
from src.ingestion.connectors.postgres.postgres import get_postgres_engine

# ====== SETTING ======
SCHEMAS = [
    "bronze",
    "silver",
    "feature",
    "gold"
]

# ====== CREATE SCHEMAS ======
def create_schemas():
    engine = get_postgres_engine()
    with engine.connect() as conn:
        for schema in SCHEMAS:
            conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {schema};"
            )
        conn.commit()