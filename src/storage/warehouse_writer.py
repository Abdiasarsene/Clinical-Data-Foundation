# src/storage/warehouse_writer.py
from storage.base_writer import BaseWriter
from storage.modes import WriteMode


class WarehouseWriter(BaseWriter):
    """
    Loads data into SQL warehouse.
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

        pandas_df = df.to_pandas() if hasattr(df, "to_pandas") else df

        if_exists = "append"

        if mode == WriteMode.OVERWRITE:
            if_exists = "replace"

        pandas_df.to_sql(
            name=self.target,
            con=self.engine,
            if_exists=if_exists,
            index=False,
        )