# src/orchestration/pipelines/run_ingestion.py
from observability.monitoring.events import PipelineEvent
from observability.monitoring.exceptions import PipelineFailed
from observability.monitoring.reporter import report
from observability.logs.logger import get_logger
from storage.writer_factory import WriterFactory
from storage.modes import WriteMode
from src.ingestion.schemas.local_to_minio import ensure_bucket, upload_directory
from src.ingestion.schemas.minio_to_postgres import MinioToPostgresIngestor
from src.ingestion.connectors.minio.create_buckets import store_raw_data_healthcare
from utils.config import settings

# ===== LOGGING =====
logger = get_logger("pipeline.ingestion")

# ===== SETTINGS =====
PIPELINE_NAME = "ingestion"
RAW_TABLE = "healthcare_raw_data"


# ===== RUN INGESTION =====
def run_ingestion():
    report(PipelineEvent.START, pipeline=PIPELINE_NAME)
    logger.info("Starting ingestion pipeline")

    try:

        # ===== INIT MINIO CLIENT =====
        logger.info("Initializing MinIO client")
        minio_client = store_raw_data_healthcare()

        # ===== LOCAL → MINIO =====
        logger.info("Uploading local data to MinIO")
        ensure_bucket(minio_client)
        upload_directory(minio_client)

        report(PipelineEvent.STAGE_SUCCESS, pipeline=PIPELINE_NAME, stage="local_to_minio")

        # ===== MINIO → POSTGRES =====
        logger.info("Starting ingestion from MinIO to Postgres")
        ingestor = MinioToPostgresIngestor(
            minio_client=minio_client,
            pg_conn_str=settings.POSTGRES_URI,
            raw_schema="raw"
        )
        df = ingestor.ingest_bucket(RAW_TABLE)

        # ===== WRITE USING STORAGE LAYER =====
        writer = WriterFactory.get_writer(
            layer="raw",
            engine=None,  # MinioToPostgresIngestor already writes to PG
            target=RAW_TABLE,
        )
        writer.write(
            df,
            mode=WriteMode.OVERWRITE,
        )
        report(PipelineEvent.STAGE_SUCCESS, pipeline=PIPELINE_NAME, stage="minio_to_postgres")
        logger.info("Ingestion pipeline completed")
        report(PipelineEvent.SUCCESS, pipeline=PIPELINE_NAME)

    except Exception as e:
        logger.exception("Ingestion pipeline failed")
        report(PipelineEvent.FAILURE, pipeline=PIPELINE_NAME, error=str(e))
        raise PipelineFailed("Ingestion pipeline failed") from e


if __name__ == "__main__":
    run_ingestion()