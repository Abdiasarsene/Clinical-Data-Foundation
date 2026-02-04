# monitoring/reporter.py
from logs.logger import get_logger

logger = get_logger("monitoring")

def report(event, **metrics):
    logger.info(event, extra=metrics)