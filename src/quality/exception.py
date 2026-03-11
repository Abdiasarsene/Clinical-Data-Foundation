# orchestration/quality/exception.py
class DataQualityFailure(Exception):

    def __init__(self, message: str, asset: str | None = None):
        self.asset = asset
        super().__init__(message)