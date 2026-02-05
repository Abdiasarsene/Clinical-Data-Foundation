# src/transformations/silver/enrich_struct.py
import polars as pl
import logging

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== ADD INGESTION METADATA ======
def add_ingestion_metadata(
    df: pl.LazyFrame,
    source_name: str,
) -> pl.LazyFrame:
    try:
        return df.with_columns([
            pl.lit(source_name).alias("_source"),
            pl.lit(pl.datetime_now()).alias("_ingested_at")
        ])
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise

# ====== ADD TIME DURATION ======
def add_time_dimensions(
    df: pl.LazyFrame,
    date_column: str,
) -> pl.LazyFrame:
    try:
        if date_column not in df.columns:
            return df

        return df.with_columns([
            pl.col(date_column).dt.year().alias("year"),
            pl.col(date_column).dt.month().alias("month"),
            pl.col(date_column).dt.weekday().alias("weekday"),
        ])
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise

# ====== ADD ROW FINGERPRINT ======
def add_row_fingerprint(df: pl.LazyFrame) -> pl.LazyFrame:
    try:
        return df.with_columns(
            pl.struct(df.columns)
            .hash()
            .alias("_row_hash")
        )
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise