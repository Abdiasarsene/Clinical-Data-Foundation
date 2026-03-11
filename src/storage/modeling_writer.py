# src/storage/modeling_writer.py

from storage.base_writer import BaseWriter
from storage.modes import WriteMode


class ModelingWriter(BaseWriter):
    """
    Business-facing tables.
    Strong guarantees.
    """

    def write(
        self,
        df,
        mode: WriteMode,
        *,
        primary_keys=None,
        partition_by=None,
        metadata=None,
    ):
        self._validate(df)

        if mode == WriteMode.MERGE and not primary_keys:
            raise ValueError("MERGE requires primary keys")

        self.engine.write_table(
            df=df,
            table=self.target,
            mode=mode.value,
            primary_keys=primary_keys,
            partition_by=partition_by,
            metadata=metadata,
        )