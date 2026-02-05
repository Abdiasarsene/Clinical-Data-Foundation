# src/pipelines/run_ingestion.py

from monitoring.events import PipelineEvent
from monitoring.exceptions import PipelineFailed
from monitoring.reporter import report

from ingestion.local_to_minio import (
    ensure_bucket,
    upload_directory
)
from ingestion.minio_to_postgres import MinioToPostgresIngestor
from connectors.minio.create_buckets import store_raw_data_healthcare
from utils.config import settings

def run_ingestion():
    report(PipelineEvent.START, pipeline="ingestion")

    try:
        # ====== INIT CLIENTS ======
        minio_client = store_raw_data_healthcare()

        # ====== LOCAL → MINIO ======
        ensure_bucket(minio_client)
        upload_directory(minio_client)

        # ====== MINIO → POSTGRES (RAW) ======
        ingestor = MinioToPostgresIngestor(
            minio_client=minio_client,
            pg_conn_str=settings.POSTGRES_URI,
            raw_schema="raw"
        )

        ingestor.ingest_bucket("healthcare-raw-data")

        report(
            PipelineEvent.SUCCESS,
            pipeline="ingestion"
        )

    except Exception as e:
        report(
            PipelineEvent.FAILURE,
            pipeline="ingestion",
            error=str(e)
        )
        raise PipelineFailed("Ingestion pipeline failed") from e


if __name__ == "__main__":
    run_ingestion()