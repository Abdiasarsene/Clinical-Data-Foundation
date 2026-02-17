# src/transformations/silver/validate.py
import polars as pl
from typing import Dict, Iterable

# ====== VALIDATE NOT EMPTY ======
def validate_not_empty(df: pl.LazyFrame) -> pl.LazyFrame:
    if df.select(pl.len()).collect().item() == 0:
        raise ValueError("Dataset is empty")
    return df

# ====== VALIDATE SCHEMA STABILITY ======
def validate_schema_stability(
    df: pl.LazyFrame,
    expected_columns: Iterable[str] | None = None,
) -> pl.LazyFrame:
    if expected_columns:
        missing = set(expected_columns) - set(df.columns)
        extra = set(df.columns) - set(expected_columns)

        if missing:
            raise ValueError(f"Missing columns detected: {missing}")
        if extra:
            raise ValueError(f"Unexpected columns detected: {extra}")

    return df

# ====== VALIDATE UNIQUENESS ======
def validate_uniqueness(
    df: pl.LazyFrame,
    technical_keys: list[str],
) -> pl.LazyFrame:
    dup_count = (
        df.select(technical_keys)
        .collect()
        .is_duplicated()
        .sum()
    )

    if dup_count > 0:
        raise ValueError(f"Detected {dup_count} duplicate technical keys")

    return df
