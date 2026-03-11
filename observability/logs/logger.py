# orchestration/observability/logs/logger.py
import logging
from pythonjsonlogger import jsonlogger

def get_logger(name: str) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:

        handler = logging.StreamHandler()

        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )

        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger