# orchestration/observability/monitoring/exceptions.py
class PipelineFailed(Exception):

    def __init__(self, message: str, stage: str | None = None):
        self.stage = stage
        super().__init__(message)