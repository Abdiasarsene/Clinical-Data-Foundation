from abc import ABC, abstractmethod
from storage.modes import WriteMode

class BaseWriter(ABC):

    def __init__(self, engine, target):
        self.engine = engine
        self.target = target

    @abstractmethod
    def write(
        self,
        df,
        mode: WriteMode,
        *,
        primary_keys=None,
        partition_by=None,
        metadata=None
    ):
        pass

    def _validate(self, df):
        if df is None or df.empty:
            raise ValueError("Cannot write empty dataframe")