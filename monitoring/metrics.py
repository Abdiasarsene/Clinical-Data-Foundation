# monitoring/metrics.py
def compute_row_metrics(engine, df):
    return {
        "row_count": engine.row_count(df)
    }