# src/storage/base_writer.py

from abc import ABC, abstractmethod


class BaseWriter(ABC):

    def __init__(self, engine, target: str):
        self.engine = engine
        self.target = target

    @abstractmethod
    def write(
        self,
        df,
        mode,
        *,
        primary_keys=None,
        partition_by=None,
        metadata=None,
    ):
        pass

    def _validate(self, df):

        if df is None:
            raise ValueError("Cannot write None dataframe")

        if hasattr(df, "empty") and df.empty:
            raise ValueError("Cannot write empty dataframe")