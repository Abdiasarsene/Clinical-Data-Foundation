# src/quality/context.py
from pathlib import Path
from functools import lru_cache
import great_expectations as gx

# ====== GET CONTEXT ======
@lru_cache
def get_ge_context():
    project_root = Path(__file__).resolve().parents[2]
    ge_root = project_root / "great_expectations"
    return gx.get_context(context_root_dir=str(ge_root))