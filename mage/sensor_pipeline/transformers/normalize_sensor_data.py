import os
import time
from pathlib import Path

import duckdb

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer


VALID_ROW_FILTER = """
    department_name IS NOT NULL
    AND sensor_serial IS NOT NULL
    AND product_name IS NOT NULL
    AND create_at IS NOT NULL
    AND product_expire IS NOT NULL
"""


def _sql_string(value: str) -> str:
    """Quote a string literal for DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB")
    threads = int(os.environ.get("DUCKDB_THREADS", "4"))
    con.execute(f"SET memory_limit = {_sql_string(memory_limit)}")
    con.execute(f"SET threads = {threads}")


@transformer
def normalize_sensor_data(manifest: dict, **kwargs) -> dict:
    """
    Prepare parquet for fast Postgres loading without creating Mage child blocks.

    This block runs once. It optionally compacts 43k minute parquet files into
    daily parquet files, then returns small metadata only. It never returns the
    82M-row fact data as a pandas DataFrame.
    """
    source_dir = manifest["source_dir"]
    days = manifest.get("days", [])
    compact_daily_parquet = manifest.get("compact_daily_parquet", True)
    prepared_dir = Path(manifest["prepared_dir"])

    if not days:
        return {
            **manifest,
            "daily_files": [],
            "expected_rows": 0,
            "source_min_create_at": None,
            "source_max_create_at": None,
        }

    prepared_dir.mkdir(parents=True, exist_ok=True)

    daily_files = []
    expected_rows = 0
    source_min_create_at = None
    source_max_create_at = None

    con = duckdb.connect()
    _configure_duckdb(con)

    try:
        for index, day in enumerate(days, start=1):
            t0 = time.time()
            source_glob = f"{source_dir}/{day}*.parquet"

            if compact_daily_parquet:
                load_path = str(prepared_dir / f"{day}.parquet")
                tmp_load_path = f"{load_path}.tmp"
                Path(tmp_load_path).unlink(missing_ok=True)

                # Keep only valid rows here, matching the final fact-table rules.
                con.execute(
                    f"""
                    COPY (
                        SELECT
                            department_name,
                            sensor_serial,
                            product_name,
                            create_at,
                            product_expire
                        FROM read_parquet(?)
                        WHERE {VALID_ROW_FILTER}
                    ) TO {_sql_string(tmp_load_path)} (
                        FORMAT PARQUET,
                        COMPRESSION 'SNAPPY'
                    )
                    """,
                    [source_glob],
                )
                Path(tmp_load_path).replace(load_path)
            else:
                load_path = source_glob

            stats = con.execute(
                f"""
                SELECT
                    count(*) AS row_count,
                    min(create_at) AS min_create_at,
                    max(create_at) AS max_create_at
                FROM read_parquet(?)
                WHERE {VALID_ROW_FILTER}
                """,
                [load_path],
            ).fetchone()

            row_count = int(stats[0] or 0)
            min_create_at = stats[1]
            max_create_at = stats[2]

            expected_rows += row_count
            if min_create_at is not None:
                source_min_create_at = (
                    min_create_at
                    if source_min_create_at is None
                    else min(source_min_create_at, min_create_at)
                )
            if max_create_at is not None:
                source_max_create_at = (
                    max_create_at
                    if source_max_create_at is None
                    else max(source_max_create_at, max_create_at)
                )

            daily_files.append(
                {
                    "day": day,
                    "source_glob": source_glob,
                    "load_path": load_path,
                    "row_count": row_count,
                    "min_create_at": str(min_create_at) if min_create_at else None,
                    "max_create_at": str(max_create_at) if max_create_at else None,
                }
            )

            print(
                f"Prepared {index}/{len(days)} {day}: "
                f"{row_count:,} valid rows in {time.time() - t0:.1f}s"
            )
    finally:
        con.close()

    print(
        f"Prepared {len(daily_files)} day file(s), "
        f"expected valid rows: {expected_rows:,}"
    )

    return {
        **manifest,
        "daily_files": daily_files,
        "expected_rows": expected_rows,
        "source_min_create_at": str(source_min_create_at)
        if source_min_create_at
        else None,
        "source_max_create_at": str(source_max_create_at)
        if source_max_create_at
        else None,
    }
