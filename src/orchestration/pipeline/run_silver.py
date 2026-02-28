# src/orchestration/pipeline/run_silver.py
import time
from lake.engine.polars_engine import PolarsEngine
from lake.silver.clean import clean_table
from lake.silver.normalize import (
    normalize_strings,
    normalize_dates,
    normalize_numeric_types,
)
from lake.silver.validate import (
    validate_not_empty,
)
from lake.silver.enrich_struct import (
    add_ingestion_metadata,
    add_time_dimensions,
    add_row_fingerprint,
)
from monitoring.events import PipelineEvent
from monitoring.metrics import compute_row_metrics
from monitoring.reporter import report
from monitoring.exceptions import PipelineFailed
from logs.logger import get_logger

# ====== LOGGING ======
logger = get_logger("pipeline.silver.data")

# ====== PIPELINE FOR RAW DATA TO SILVER ======
def run():
    start_time = time.time()
    engine = PolarsEngine()

    dataset = "sales"
    stage = "silver"
    source_path = "lake_tabulaire/sales_raw/"
    target_path = "lake/silver/sales/"

    try:
        # ===== PIPELINE START =====
        report(PipelineEvent.START, dataset=dataset, stage=stage)
        logger.info("Silver pipeline started", extra={
            "dataset": dataset,
            "stage": stage,
            "source_path": source_path,
        })

        # ===== READ =====
        df = engine.read(source_path)

        metrics_in = compute_row_metrics(engine, df)
        report("ROWS_IN", **metrics_in)

        # ===== VALIDATE BRONZE =====
        df = validate_not_empty(df)

        # ===== CLEAN =====
        df = clean_table(df)

        # ===== NORMALIZE =====
        df = normalize_strings(df)
        df = normalize_dates(df)
        df = normalize_numeric_types(df)

        # ===== ENRICH =====
        df = add_ingestion_metadata(df, source_name="sales_api")
        df = add_row_fingerprint(df)

        # Optional (analytics-ready)
        df = add_time_dimensions(df, date_column="date")

        # ===== METRICS OUT =====
        metrics_out = compute_row_metrics(engine, df)
        report("ROWS_OUT", **metrics_out)

        # ===== WRITE SILVER =====
        engine.write(df, target_path)

        # ===== PIPELINE SUCCESS =====
        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.SUCCESS,
            dataset=dataset,
            stage=stage,
            duration_sec=duration,
            rows_in=metrics_in["row_count"],
            rows_out=metrics_out["row_count"],
        )

        logger.info("Silver pipeline finished successfully", extra={
            "duration_sec": duration,
        })

    except Exception as e:
        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.FAILURE,
            dataset=dataset,
            stage=stage,
            duration_sec=duration,
            error=str(e),
        )

        logger.error("Silver pipeline failed", extra={
            "error": str(e),
            "duration_sec": duration,
        })

        raise PipelineFailed(str(e))


if __name__ == "__main__":
    run()
