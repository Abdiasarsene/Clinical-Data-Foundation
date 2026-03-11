# orchestration/quality/context.py
from pathlib import Path
from functools import lru_cache
from great_expectations.data_context import FileDataContext

@lru_cache
def get_ge_context() -> FileDataContext:

    project_root = Path(__file__).resolve().parents[2]
    ge_root = project_root / "great_expectations"

    return FileDataContext(context_root_dir=str(ge_root))