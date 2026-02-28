# src/modeling/temporal/scd.py 
from datetime import datetime

def apply_scd_type2(existing_record, new_record):
    """
    Close previous record and create new version.
    """

    existing_record["valid_to"] = datetime.utcnow()
    new_record["valid_from"] = datetime.utcnow()
    new_record["valid_to"] = None

    return existing_record, new_record