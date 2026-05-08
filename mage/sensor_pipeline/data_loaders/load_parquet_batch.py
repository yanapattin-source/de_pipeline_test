import os
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_parquet_batch(**kwargs) -> dict:
    """
    Load one day of Parquet files from data_sample/ directory.
    
    Mage triggers this pipeline once per day (31 runs for Jan 2023).
    The 'day' parameter is passed from the pipeline run.
    """
    day = kwargs.get("day", "2023-01-01")
    data_dir = "/home/src/data_sample"

    # Find all parquet files for this day
    files = sorted([
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.startswith(day) and f.endswith(".parquet")
    ])

    if not files:
        print(f"No files found for {day}")
        return {"df": pd.DataFrame(), "day": day}

    # Read and concatenate all minute-files for this day
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    print(f"Loaded {len(df)} rows from {len(files)} files for {day}")
    return {"df": df, "day": day}