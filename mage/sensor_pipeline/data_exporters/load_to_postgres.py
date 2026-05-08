import os
import pandas as pd
from sqlalchemy import bindparam, create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

DB_URL = os.environ.get(
    "MAGE_DATABASE_CONNECTION_URL",
    "postgresql+psycopg2://pipeline:pipeline123@postgres:5432/sensor_db",
)

engine = create_engine(DB_URL)
metadata = MetaData()


def get_or_create_ids(
    engine,
    table_name: str,
    name_col: str,
    id_col: str,
    names: list,
) -> dict:
    """Upsert dimension rows and return {name: id} mapping."""
    if not names:
        return {}

    tbl = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as conn:
        # Parameterized bulk upsert — no SQL injection risk
        stmt = insert(tbl).values([{name_col: n} for n in names])
        stmt = stmt.on_conflict_do_nothing(index_elements=[name_col])
        conn.execute(stmt)

        # Fetch all IDs for the names we care about
        query = text(
            f"SELECT {name_col}, {id_col} FROM {table_name} "
            f"WHERE {name_col} IN :names"
        ).bindparams(bindparam("names", expanding=True))
        result = conn.execute(query, {"names": names})

        return {row[0]: row[1] for row in result}


@data_exporter
def load_to_postgres(data: dict, **kwargs) -> dict:
    """
    Load dimension and fact data into PostgreSQL star schema.
    Uses upsert pattern: insert new dimension rows, then bulk-insert facts.
    """
    # Skip if no data for this day
    if data["fact"].empty:
        print(f"No fact data for {data['day']}, skipping.")
        return {"loaded_rows": 0, "day": data["day"]}

    # --- Upsert dimensions ---
    dept_names = data["dim_department"]["department_name"].tolist()
    dept_map = get_or_create_ids(
        engine, "dim_department", "department_name", "department_id", dept_names,
    )

    product_names = data["dim_product"]["product_name"].tolist()
    product_map = get_or_create_ids(
        engine, "dim_product", "product_name", "product_id", product_names,
    )

    # --- Upsert sensors (need department_id) ---
    sensor_df = data["dim_sensor"].copy()
    sensor_df["department_id"] = sensor_df["department_name"].map(dept_map)

    dim_sensor_tbl = Table("dim_sensor", metadata, autoload_with=engine)
    with engine.begin() as conn:
        sensor_records = sensor_df[["sensor_serial", "department_id"]].to_dict("records")
        stmt = insert(dim_sensor_tbl).values(sensor_records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["sensor_serial", "department_id"],
        )
        conn.execute(stmt)

        # Build sensor map using composite key
        serials = sensor_df["sensor_serial"].unique().tolist()
        query = text(
            "SELECT sensor_serial, department_id, sensor_id FROM dim_sensor "
            "WHERE sensor_serial IN :serials"
        ).bindparams(bindparam("serials", expanding=True))
        result = conn.execute(query, {"serials": serials})
        sensor_map = {(row[0], row[1]): row[2] for row in result}

    # --- Build fact DataFrame with foreign keys ---
    fact_df = data["fact"].copy()
    fact_df["department_id"] = fact_df["department_name"].map(dept_map)
    fact_df["sensor_id"] = fact_df.apply(
        lambda r: sensor_map.get((r["sensor_serial"], r["department_id"])), axis=1,
    )
    fact_df["product_id"] = fact_df["product_name"].map(product_map)

    # Validate all foreign keys resolved
    missing = fact_df[["sensor_id", "product_id", "department_id"]].isna().any(axis=1)
    if missing.any():
        raise ValueError(
            f"{missing.sum()} rows have missing foreign keys — dimension upsert failed"
        )

    fact_df = fact_df[["sensor_id", "product_id", "department_id",
                        "create_at", "product_expire"]]

    # Bulk insert facts using pandas to_sql
    fact_df.to_sql(
        "fact_sensor_reading", engine,
        if_exists="append", index=False,
        method="multi", chunksize=50000,
    )

    print(f"Loaded {len(fact_df)} fact rows for {data['day']}")
    return {"loaded_rows": len(fact_df), "day": data["day"]}
