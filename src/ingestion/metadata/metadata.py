# src/ingestion/metadata.py
from datetime import datetime
from hashlib import md5
from pathlib import Path

# ====== BUILD METADATA ======
def build_metadata(
    source_path: str,
    bucket: str,
    object_name: str,
    source_type: str
) -> dict:
    path = Path(source_path)

    return {
        "source_type": source_type,          # local | api | cdc
        "original_filename": path.name,
        "original_extension": path.suffix,
        "minio_bucket": bucket,
        "minio_object": object_name,
        "ingestion_ts": datetime.utcnow(),
        "dataset_id": md5(object_name.encode()).hexdigest()
    }