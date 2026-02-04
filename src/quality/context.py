from great_expectations.data_context import FileDataContext
from pathlib import Path

def get_ge_context():
    project_root = Path(__file__).resolve().parents[1]
    ge_root = project_root / "great_expectations"
    return FileDataContext(context_root_dir=str(ge_root))