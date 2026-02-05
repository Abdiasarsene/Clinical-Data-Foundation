# src/transformations/silver/clean.py
import polars as pl
import logging 

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== CLEAN TABLE ======
def clean_table(df: pl.LazyFrame, id_columns: list = None) -> pl.LazyFrame:
    try:
        # Duplication
        if id_columns:
            df = df.unique(subset=id_columns)

        # Simplified automatic typing
        for col_name, dtype in df.schema.items():
            if dtype == pl.Utf8 and "date" in col_name.lower():
                df = df.with_columns(pl.col(col_name).str.strptime(pl.Date, strict=False))
            if dtype == pl.Int64:
                df = df.with_columns(pl.col(col_name).cast(pl.Float64))

        # Suppression nulls techniques
        df = df.drop_nulls() 

        # Filtering technical outliers
        for col_name, dtype in df.schema.items():
            if dtype in [pl.Int64, pl.Float64]:
                df = df.filter(pl.col(col_name) >= 0)
        return df
    
    except Exception as e:
        logger.error(f"❌ Error Detected : {e}", exc_info=True)
        raise e