import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def normalize_sensor_data(data: dict, **kwargs) -> dict:
    """
    Split raw sensor data into dimension and fact DataFrames
    for star schema loading.
    
    Input: {"df": DataFrame, "day": str}
    Output: {
        "dim_department": DataFrame,
        "dim_sensor": DataFrame, 
        "dim_product": DataFrame,
        "fact": DataFrame,
        "day": str
    }
    """
    df = data["df"]
    day = data["day"]

    if df.empty:
        return {
            "dim_department": pd.DataFrame(),
            "dim_sensor": pd.DataFrame(),
            "dim_product": pd.DataFrame(),
            "fact": pd.DataFrame(),
            "day": day,
        }

    # Extract unique dimensions
    dim_department = df[["department_name"]].drop_duplicates()
    dim_sensor = df[["department_name", "sensor_serial"]].drop_duplicates()
    dim_product = df[["product_name"]].drop_duplicates()

    # Fact table — all columns (IDs will be resolved during load via upsert)
    fact = df[["department_name", "sensor_serial", "product_name", 
               "create_at", "product_expire"]].copy()

    return {
        "dim_department": dim_department,
        "dim_sensor": dim_sensor,
        "dim_product": dim_product,
        "fact": fact,
        "day": day,
    }