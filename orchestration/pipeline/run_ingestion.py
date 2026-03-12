# src/orchestration/pipelines/run_ingestion.py
from observability.monitoring.events import PipelineEvent
from observability.monitoring.exceptions import PipelineFailed
from observability.monitoring.reporter import report
from observability.logs.logger import get_logger

from src.ingestion.connectors.postgres.init_postgres import create_schemas
from src.ingestion.schemas.local_to_minio import ensure_bucket, upload_directory
from src.ingestion.schemas.minio_to_postgres import MinioToPostgresIngestor
from src.ingestion.connectors.minio.create_buckets import store_raw_data_healthcare
from utils.config import settings

# ===== LOGGING =====
logger = get_logger("pipeline.ingestion")

# ===== SETTINGS =====
PIPELINE_NAME = "ingestion"
BRONZE_SCHEMA = "bronze"
RAW_BUCKET = "healthcare-raw-data"

# ===== RUN INGESTION =====
def run_ingestion():

    report(PipelineEvent.START, pipeline=PIPELINE_NAME)
    logger.info("Starting ingestion pipeline")

    try:
        # ===== INIT POSTGRES & SCHEMAS =====
        logger.info("Initializing Postgres and creating schemas")
        create_schemas()
        report(PipelineEvent.STAGE_SUCCESS, pipeline=PIPELINE_NAME, stage="postgres_init")

        # ===== INIT MINIO CLIENT =====
        logger.info("Initializing MinIO client")
        minio_client = store_raw_data_healthcare()

        # ===== LOCAL → MINIO =====
        logger.info("Uploading local datasets to MinIO")
        ensure_bucket(minio_client)
        upload_directory(minio_client)
        report(PipelineEvent.STAGE_SUCCESS, pipeline=PIPELINE_NAME, stage="local_to_minio")

        # ===== MINIO → POSTGRES (BRONZE) =====
        logger.info("Ingesting datasets from MinIO to Postgres (bronze)")
        ingestor = MinioToPostgresIngestor(
            minio_client=minio_client,
            pg_conn_str=settings.POSTGRES_URI,
            raw_schema=BRONZE_SCHEMA
        )
        ingestor.ingest_bucket(RAW_BUCKET)
        report(PipelineEvent.STAGE_SUCCESS, pipeline=PIPELINE_NAME, stage="minio_to_postgres")

        logger.info("Ingestion pipeline completed successfully")
        report(PipelineEvent.SUCCESS, pipeline=PIPELINE_NAME)

    except Exception as e:
        logger.exception("Ingestion pipeline failed")
        report(PipelineEvent.FAILURE, pipeline=PIPELINE_NAME, error=str(e))
        raise PipelineFailed("Ingestion pipeline failed") from e


if __name__ == "__main__":
    run_ingestion()