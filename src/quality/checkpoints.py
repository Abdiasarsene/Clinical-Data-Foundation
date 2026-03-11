# orchestration/quality/checkpoints.py
from orchestration.quality.context import get_ge_context
from orchestration.quality.exception import DataQualityFailure

def run_checkpoint(name: str):

    context = get_ge_context()

    result = context.run_checkpoint(checkpoint_name=name)

    if not result["success"]:
        raise DataQualityFailure(f"Checkpoint failed: {name}")

    return result