# src/transformation/engine/polars_engine.py
import polars as pl

# ====== POLARS ENGINE CLASS ======
class PolarsEngine:
    def read(self, path: str) -> pl.LazyFrame:
        return pl.scan_parquet(path)

    def write(self, df: pl.LazyFrame, path: str):
        df.collect().write_parquet(path)
    
    def row_count(self, df: pl.LazyFrame) -> int:
        return df.select(pl.len()).collect().item()