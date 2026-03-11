# orchestration/observability/monitoring/events.py
from enum import Enum

class PipelineEvent(str, Enum):
    START = "pipeline_start"
    SUCCESS = "pipeline_success"
    FAILURE = "pipeline_failure"

    STAGE_START = "stage_start"
    STAGE_SUCCESS = "stage_success"
    STAGE_FAILURE = "stage_failure"