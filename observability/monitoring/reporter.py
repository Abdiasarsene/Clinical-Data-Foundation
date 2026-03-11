# orchestration/observability/monitoring/reporter.py
from datetime import datetime
from logs.logger import get_logger

# ====== LOGGING ======
logger = get_logger("monitoring")

def report(event: str, pipeline: str, stage: str | None = None, **metrics):

    payload = {
        "event": event,
        "pipeline": pipeline,
        "stage": stage,
        "timestamp": datetime.utcnow().isoformat(),
        **metrics,
    }

    logger.info(event, extra=payload)