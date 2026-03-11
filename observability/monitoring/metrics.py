# orchestration/observability/monitoring/metrics.py
from typing import Dict, Any

def compute_row_metrics(engine, df) -> Dict[str, Any]:

    metrics = {}

    try:
        metrics["row_count"] = engine.row_count(df)
    except Exception:
        metrics["row_count"] = None

    return metrics