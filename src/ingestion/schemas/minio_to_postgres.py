# src/ingestion/minio_to_postgres.py
import io
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine
from pathlib import Path

SUPPORTED_FORMATS = {".csv", ".parquet", ".xlsx", ".xls"}

# ====== INGESTION MINIO TO POSTGRES ======
class MinioToPostgresIngestor:
    def __init__(
        self,
        minio_client: Minio,
        pg_conn_str: str,
        raw_schema: str = "raw"
    ):
        self.minio = minio_client
        self.engine = create_engine(pg_conn_str)
        self.raw_schema = raw_schema

    def ingest_bucket(self, bucket: str):
        objects = self.minio.list_objects(bucket, recursive=True)

        for obj in objects:
            suffix = Path(obj.object_name).suffix.lower()

            if suffix not in SUPPORTED_FORMATS:
                continue  # on ignore les formats non tabulaires

            df = self._read_object(bucket, obj.object_name, suffix)
            table_name = self._normalize_table_name(obj.object_name)

            df.to_sql(
                table_name,
                self.engine,
                schema=self.raw_schema,
                if_exists="replace",
                index=False
            )

    def _read_object(self, bucket: str, object_name: str, suffix: str) -> pd.DataFrame:
        response = self.minio.get_object(bucket, object_name)
        data = response.read()

        if suffix == ".csv":
            return pd.read_csv(io.BytesIO(data))

        if suffix == ".parquet":
            return pd.read_parquet(io.BytesIO(data))

        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(io.BytesIO(data))

        raise ValueError(f"Unsupported format: {suffix}")

    @staticmethod
    def _normalize_table_name(object_name: str) -> str:
        return (
            Path(object_name)
            .stem
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
        )