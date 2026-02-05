from storage.base_writer import BaseWriter
from storage.modes import WriteMode

class LakeWriter(BaseWriter):
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
        metadata=None
    ):
        self._validate(df)

        self.engine.write_table(
            df=df,
            table=self.target,
            mode=mode.value,
            partition_by=partition_by,
            metadata=metadata,
        )