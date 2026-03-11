# src/storage/lake_writer.py
from storage.base_writer import BaseWriter
from storage.modes import WriteMode

class SilverWriter(BaseWriter):
    """
    Technical curated layer.
    Stable schema, no business meaning.
    """

    def write(
        self,
        df,
        mode: WriteMode,
        *,
        partition_by=None,
        metadata=None,
        primary_keys=None,
    ):
        self._validate(df)

        self.engine.write_table(
            df=df,
            table=self.target,
            mode=mode.value,
            partition_by=partition_by,
            metadata=metadata,
        )