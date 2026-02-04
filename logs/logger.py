# logs/logger.py
import logging
from pythonjsonlogger import jsonlogger

# ====== GET LOGGER ======
def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(extra)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger