# src/transformations/silver/normalize.py
import polars as pl
import logging

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== NORMALIZE STRINGS ======
def normalize_strings(df: pl.LazyFrame) -> pl.LazyFrame:
    try:
        return df.with_columns(
            [
                pl.col(col)
                .str.strip()
                .str.to_lowercase()
                for col, dtype in df.schema.items()
                if dtype == pl.Utf8
            ]
        )
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise e

# ====== NORMALIZE DATES ======
def normalize_dates(df: pl.LazyFrame) -> pl.LazyFrame:
    try:
        for col, dtype in df.schema.items():
            if dtype == pl.Utf8 and "date" in col.lower():
                df = df.with_columns(
                    pl.col(col).str.strptime(pl.Date, strict=False)
                )
        return df
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise e

# ====== NORMALIZE NUMERIC TYPES ======
def normalize_numeric_types(df: pl.LazyFrame) -> pl.LazyFrame:
    try:
        return df.with_columns(
            [
                pl.col(col).cast(pl.Float64)
                for col, dtype in df.schema.items()
                if dtype in (pl.Int32, pl.Int64)
            ]
        )
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise e