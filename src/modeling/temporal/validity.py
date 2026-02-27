# src/modeling/temporal/validity.py 
def validate_temporal_order(start, end):
    if end and end < start:
        raise ValueError("Invalid temporal order")