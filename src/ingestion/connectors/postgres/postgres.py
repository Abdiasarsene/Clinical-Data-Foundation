# src/ingestion/cnnectors/postgres/postgres.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ====== SETTING ======
load_dotenv()

# ====== CONNECTION ======
def get_postgres_engine():
    conn_str = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    engine = create_engine(conn_str)
    return engine