# src/orchestration/pipelines/run_warehouse.py
import time
from pathlib import Path
from src.lake.engine.polars_engine import PolarsEngine
from storage.writer_factory import WriterFactory
from storage.modes import WriteMode
from utils.config import settings
from observability.monitoring.events import PipelineEvent
from observability.monitoring.reporter import report
from observability.monitoring.exceptions import PipelineFailed
from observability.logs.logger import get_logger

# ===== LOGGING =====
logger = get_logger("pipeline.warehouse")

# ===== SETTINGS =====
DATASET = "healthcare"
STAGE = "warehouse"
MODELING_PATH = "lake/modeling/"

# ====== DISCOVER TABLES ======
def discover_tables(path):
    root = Path(path)
    return [folder.name for folder in root.iterdir() if folder.is_dir()]

# ======= RUN WAREHOUSE ======
def run():
    start_time = time.time()
    engine = PolarsEngine()

    try:
        report(PipelineEvent.START, dataset=DATASET, stage=STAGE)
        logger.info("Warehouse loader started")

        # ===== DISCOVER TABLES =====
        tables = discover_tables(MODELING_PATH)
        logger.info("Discovered tables", extra={"tables": tables})

        # ===== LOAD EACH TABLE USING WriterFactory =====
        for table in tables:
            logger.info("Loading table", extra={"table": table})
            df = engine.read(f"{MODELING_PATH}{table}/")
            writer = WriterFactory.get_writer(
                layer="warehouse",
                engine=engine,
                target=table,
            )
            writer.write(df, mode=WriteMode.OVERWRITE)
        duration = round(time.time() - start_time, 2)
        report(
            PipelineEvent.SUCCESS,
            dataset=DATASET,
            stage=STAGE,
            duration_sec=duration,
            tables_loaded=len(tables),
        )
        logger.info(
            "Warehouse loading completed",
            extra={"duration_sec": duration, "tables_loaded": len(tables)},
        )

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        report(
            PipelineEvent.FAILURE,
            dataset=DATASET,
            stage=STAGE,
            duration_sec=duration,
            error=str(e),
        )
        logger.exception("Warehouse loader failed")
        raise PipelineFailed(str(e)) from e


if __name__ == "__main__":
    run()