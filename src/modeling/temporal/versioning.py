# src/modeling/temporal/versioning.py 
from datetime import datetime

def apply_versioning(record, version: int):
    return {
        **record,
        "version": version,
        "versioned_at": datetime.utcnow()
    }