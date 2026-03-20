# src/orchestration/pipelines/run_silver.py
import time
from src.lake.engine.polars_engine import PolarsEngine
from src.lake.silver.clean import clean_table
from src.lake.silver.normalize import (
    normalize_strings,
    normalize_dates,
    normalize_numeric_types,
)
from src.lake.silver.validate import validate_not_empty
from src.lake.silver.enrich_struct import (
    add_ingestion_metadata,
    add_time_dimensions,
    add_row_fingerprint,
)
from src.storage.writer_factory import WriterFactory
from src.storage.modes import WriteMode
from src.quality.validators import validate_table
from observability.monitoring.events import PipelineEvent
from observability.monitoring.metrics import compute_row_metrics
from observability.monitoring.reporter import report
from observability.monitoring.exceptions import PipelineFailed
from observability.logs.logger import get_logger
from src.ingestion.connectors.postgres.list_bronze_tables import list_bronze_tables

# ===== LOGGING =====
logger = get_logger("pipeline.silver")

# ===== SETTINGS =====
PIPELINE_STAGE = "silver"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# ===== RUN PIPELINE =====
def run():
    engine = PolarsEngine()
    tables = list_bronze_tables()
    logger.info(f"Found {len(tables)} tables in bronze schema", extra={"tables": tables})

    for dataset in tables:
        start_time = time.time()
        target_table = dataset  # même nom dans silver

        try:
            report(PipelineEvent.START, dataset=dataset, stage=PIPELINE_STAGE)
            logger.info(
                f"Silver pipeline started for {dataset}",
                extra={"dataset": dataset, "stage": PIPELINE_STAGE},
            )

            # ===== READ =====
            df = engine.read(f"{BRONZE_SCHEMA}.{dataset}")
            metrics_in = compute_row_metrics(engine, df)

            report(
                PipelineEvent.METRICS,
                dataset=dataset,
                stage=PIPELINE_STAGE,
                rows_in=metrics_in["row_count"],
            )

            # ===== VALIDATION =====
            df = validate_not_empty(df)

            # ===== CLEAN =====
            df = clean_table(df)

            # ===== NORMALIZATION =====
            df = normalize_strings(df)
            df = normalize_dates(df)
            df = normalize_numeric_types(df)

            # ===== ENRICHMENT =====
            df = add_ingestion_metadata(df, source_name="bronze")
            df = add_row_fingerprint(df)
            df = add_time_dimensions(df, date_column="date")

            # ===== METRICS OUT =====
            metrics_out = compute_row_metrics(engine, df)
            report(
                PipelineEvent.METRICS,
                dataset=dataset,
                stage=PIPELINE_STAGE,
                rows_out=metrics_out["row_count"],
            )

            # ===== DATA QUALITY =====
            validate_table(
                datasource_name="lake",
                asset_name=target_table,
                suite_name=f"{dataset}_suite",
            )

            # ===== WRITE TO SILVER =====
            writer = WriterFactory.get_writer(
                layer="silver",
                engine=engine,
                target=target_table,
                schema=SILVER_SCHEMA,  # nouveau schema silver
            )

            writer.write(df, mode=WriteMode.OVERWRITE)

            duration = round(time.time() - start_time, 2)
            report(
                PipelineEvent.SUCCESS,
                dataset=dataset,
                stage=PIPELINE_STAGE,
                duration_sec=duration,
                rows_in=metrics_in["row_count"],
                rows_out=metrics_out["row_count"],
            )

            logger.info(
                f"Silver pipeline finished for {dataset}",
                extra={"dataset": dataset, "duration_sec": duration, "rows_out": metrics_out["row_count"]},
            )

        except Exception as e:
            duration = round(time.time() - start_time, 2)
            report(
                PipelineEvent.FAILURE,
                dataset=dataset,
                stage=PIPELINE_STAGE,
                duration_sec=duration,
                error=str(e),
            )
            logger.exception(f"Silver pipeline failed for {dataset}")
            raise PipelineFailed(str(e)) from e


if __name__ == "__main__":
    run()