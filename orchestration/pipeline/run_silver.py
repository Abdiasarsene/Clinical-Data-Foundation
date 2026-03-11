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
from storage.writer_factory import WriterFactory
from storage.modes import WriteMode
from src.quality.validators import validate_table
from observability.monitoring.events import PipelineEvent
from observability.monitoring.metrics import compute_row_metrics
from observability.monitoring.reporter import report
from observability.monitoring.exceptions import PipelineFailed
from observability.logs.logger import get_logger

# ===== LOGGING =====
logger = get_logger("pipeline.silver")

# ===== SETTINGS =====
PIPELINE_STAGE = "silver"
DATASET = "sales"

SOURCE_PATH = "lake_tabulaire/sales_raw/"
TARGET_TABLE = "sales"


# ===== RUN PIPELINE =====
def run():

    start_time = time.time()

    engine = PolarsEngine()

    try:

        # ===== PIPELINE START =====
        report(PipelineEvent.START, dataset=DATASET, stage=PIPELINE_STAGE)

        logger.info(
            "Silver pipeline started",
            extra={"dataset": DATASET, "stage": PIPELINE_STAGE},
        )

        # ===== READ =====
        df = engine.read(SOURCE_PATH)

        metrics_in = compute_row_metrics(engine, df)

        report(
            PipelineEvent.METRICS,
            dataset=DATASET,
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
        df = add_ingestion_metadata(df, source_name="sales_api")
        df = add_row_fingerprint(df)
        df = add_time_dimensions(df, date_column="date")

        # ===== METRICS OUT =====
        metrics_out = compute_row_metrics(engine, df)

        report(
            PipelineEvent.METRICS,
            dataset=DATASET,
            stage=PIPELINE_STAGE,
            rows_out=metrics_out["row_count"],
        )

        # ===== DATA QUALITY =====
        validate_table(
            datasource_name="lake",
            asset_name=TARGET_TABLE,
            suite_name="sales_suite",
        )

        # ===== WRITE USING STORAGE LAYER =====
        writer = WriterFactory.get_writer(
            layer="silver",
            engine=engine,
            target=TARGET_TABLE,
        )

        writer.write(
            df,
            mode=WriteMode.OVERWRITE,
        )

        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.SUCCESS,
            dataset=DATASET,
            stage=PIPELINE_STAGE,
            duration_sec=duration,
            rows_in=metrics_in["row_count"],
            rows_out=metrics_out["row_count"],
        )

        logger.info(
            "Silver pipeline finished",
            extra={
                "dataset": DATASET,
                "duration_sec": duration,
                "rows_out": metrics_out["row_count"],
            },
        )

    except Exception as e:

        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.FAILURE,
            dataset=DATASET,
            stage=PIPELINE_STAGE,
            duration_sec=duration,
            error=str(e),
        )

        logger.exception("Silver pipeline failed")

        raise PipelineFailed(str(e)) from e


if __name__ == "__main__":
    run()