# orchestration/quality/checkpoints.py
from src.quality.context import get_ge_context
from src.quality.exception import DataQualityFailure

# ====== CHECKPOINT ======
def run_checkpoint(name: str):
    context = get_ge_context()
    result = context.run_checkpoint(checkpoint_name=name)
    if not result["success"]:
        raise DataQualityFailure(f"Checkpoint failed: {name}")
    return result