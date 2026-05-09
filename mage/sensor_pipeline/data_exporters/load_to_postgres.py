import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

DB_URL = os.environ.get(
    "MAGE_DATABASE_CONNECTION_URL",
    "postgresql+psycopg2://pipeline:pipeline123@postgres:5432/sensor_db",
)
SCHEMA = os.environ.get("POSTGRES_SCHEMA", "warehouse")
LOAD_MODE = os.environ.get("LOAD_MODE", "incremental").lower()
FULL_RELOAD = LOAD_MODE in ("full_reload", "full", "truncate")
MANAGE_FACT_INDEXES = os.environ.get("MANAGE_FACT_INDEXES", "true").lower() in (
    "1",
    "true",
    "yes",
)
INDEX_MAINTENANCE_WORK_MEM = os.environ.get(
    "INDEX_MAINTENANCE_WORK_MEM", "1GB"
)
INDEX_MAX_PARALLEL_WORKERS = os.environ.get(
    "INDEX_MAX_PARALLEL_WORKERS", "4"
)
TRUNCATE_DIMENSIONS_ON_FULL_RELOAD = os.environ.get(
    "TRUNCATE_DIMENSIONS_ON_FULL_RELOAD",
    "true",
).lower() in ("1", "true", "yes")
VALIDATE_PER_DAY_COUNTS = os.environ.get("VALIDATE_PER_DAY_COUNTS", "true").lower() in (
    "1",
    "true",
    "yes",
)

APPLY_SCHEMA_SQL = os.environ.get("APPLY_SCHEMA_SQL", "true").lower() in (
    "1",
    "true",
    "yes",
)

engine = create_engine(DB_URL, connect_args={"options": f"-c search_path={SCHEMA}"})

VALID_ROW_FILTER = """
    department_name IS NOT NULL
    AND sensor_serial IS NOT NULL
    AND product_name IS NOT NULL
    AND create_at IS NOT NULL
    AND product_expire IS NOT NULL
"""

FACT_INDEXES = [
    (
        "idx_fact_sensor",
        f"CREATE INDEX IF NOT EXISTS idx_fact_sensor "
        f"ON {SCHEMA}.fact_sensor_reading(sensor_id)",
    ),
    (
        "idx_fact_dept",
        f"CREATE INDEX IF NOT EXISTS idx_fact_dept "
        f"ON {SCHEMA}.fact_sensor_reading(department_id)",
    ),
    (
        "idx_fact_product",
        f"CREATE INDEX IF NOT EXISTS idx_fact_product "
        f"ON {SCHEMA}.fact_sensor_reading(product_id)",
    ),
    (
        "idx_fact_create_at",
        f"CREATE INDEX IF NOT EXISTS idx_fact_create_at "
        f"ON {SCHEMA}.fact_sensor_reading(create_at)",
    ),
]


def _apply_schema_sql_if_requested() -> None:
    """Apply schema.sql DDL if APPLY_SCHEMA_SQL=true and SCHEMA_SQL_PATH is set."""
    schema_file = os.environ.get("SCHEMA_SQL_PATH")
    if not APPLY_SCHEMA_SQL or not schema_file:
        return

    with open(schema_file) as f:
        sql = f.read()

    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except ProgrammingError as e:
            # Expected on rerun: tables, constraints, or indexes already exist.
            if "already exists" in str(e):
                logging.info("DDL already applied, skipping: %s", e)
            else:
                logging.warning("DDL error, continuing if object already exists: %s", e)
        except Exception as e:
            logging.error("Unexpected DDL error: %s", e)
            raise


def _sql_string(value: str) -> str:
    """Quote a string literal for DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB")
    threads = int(os.environ.get("DUCKDB_THREADS", "4"))
    con.execute(f"SET memory_limit = {_sql_string(memory_limit)}")
    con.execute(f"SET threads = {threads}")


def _collect_and_upsert_all_dimensions(duck_con, daily_files, cursor):
    """Extract all unique dimension values from all daily parquet files, upsert once."""
    from psycopg2.extras import execute_values

    t0 = time.time()
    all_depts = set()
    all_prods = set()
    all_sensors = set()

    for item in daily_files:
        rows = duck_con.execute(
            f"""
            SELECT DISTINCT department_name, sensor_serial, product_name
            FROM read_parquet(?)
            WHERE {VALID_ROW_FILTER}
            """,
            [item["load_path"]],
        ).fetchall()
        for dept, sensor, prod in rows:
            if dept:
                all_depts.add(dept)
            if prod:
                all_prods.add(prod)
            if sensor and dept:
                all_sensors.add((sensor, dept))

    if all_depts:
        execute_values(
            cursor,
            f"INSERT INTO {SCHEMA}.dim_department (department_name) VALUES %s ON CONFLICT (department_name) DO NOTHING",
            [(d,) for d in all_depts],
        )

    if all_prods:
        execute_values(
            cursor,
            f"INSERT INTO {SCHEMA}.dim_product (product_name) VALUES %s ON CONFLICT (product_name) DO NOTHING",
            [(p,) for p in all_prods],
        )

    cursor.execute(f"SELECT department_id, department_name FROM {SCHEMA}.dim_department")
    dept_map = {row[1]: row[0] for row in cursor}

    if all_sensors:
        execute_values(
            cursor,
            f"INSERT INTO {SCHEMA}.dim_sensor (sensor_serial, department_id) VALUES %s ON CONFLICT (sensor_serial, department_id) DO NOTHING",
            [(serial, dept_map[dept]) for serial, dept in all_sensors],
        )

    print(
        f"Dimensions collected & upserted: "
        f"{len(all_depts)} departments, {len(all_prods)} products, "
        f"{len(all_sensors)} sensors in {time.time() - t0:.1f}s"
    )


def _fetch_dimension_maps(cursor):
    """Fetch all dimension ID→name mappings from Postgres."""
    cursor.execute(f"SELECT department_id, department_name FROM {SCHEMA}.dim_department")
    dept_rows = cursor.fetchall()

    cursor.execute(f"SELECT product_id, product_name FROM {SCHEMA}.dim_product")
    prod_rows = cursor.fetchall()

    cursor.execute(f"SELECT sensor_id, sensor_serial, department_id FROM {SCHEMA}.dim_sensor")
    sensor_rows = cursor.fetchall()

    return dept_rows, prod_rows, sensor_rows


def _register_dims_in_duckdb(duck_con, dept_rows, prod_rows, sensor_rows):
    """Create DuckDB in-memory dimension tables from Postgres rows."""
    duck_con.execute("DROP TABLE IF EXISTS _dept")
    duck_con.execute("CREATE TABLE _dept (department_id INTEGER, department_name VARCHAR)")
    duck_con.executemany("INSERT INTO _dept VALUES (?, ?)", [(r[0], r[1]) for r in dept_rows])

    duck_con.execute("DROP TABLE IF EXISTS _prod")
    duck_con.execute("CREATE TABLE _prod (product_id INTEGER, product_name VARCHAR)")
    duck_con.executemany("INSERT INTO _prod VALUES (?, ?)", [(r[0], r[1]) for r in prod_rows])

    duck_con.execute("DROP TABLE IF EXISTS _sensor")
    duck_con.execute("CREATE TABLE _sensor (sensor_id INTEGER, sensor_serial VARCHAR, department_id INTEGER)")
    duck_con.executemany("INSERT INTO _sensor VALUES (?, ?, ?)", [(r[0], r[1], r[2]) for r in sensor_rows])


def _duckdb_resolve_and_copy(duck_con, parquet_path, cursor, csv_dir, day):
    """
    DuckDB reads parquet → JOINs dims → native CSV → Postgres COPY.
    DuckDB's C++ CSV writer, no Python csv overhead.
    Returns row count inserted.
    """
    csv_path = f"{csv_dir}/{day}.csv"

    duck_con.execute(
        f"""
        COPY (
            SELECT
                s.sensor_id,
                p.product_id,
                d.department_id,
                raw.create_at,
                raw.product_expire
            FROM read_parquet(?) AS raw
            JOIN _dept d ON d.department_name = raw.department_name
            JOIN _prod p ON p.product_name = raw.product_name
            JOIN _sensor s ON s.sensor_serial = raw.sensor_serial
                AND s.department_id = d.department_id
            WHERE raw.department_name IS NOT NULL
                AND raw.sensor_serial IS NOT NULL
                AND raw.product_name IS NOT NULL
                AND raw.create_at IS NOT NULL
                AND raw.product_expire IS NOT NULL
        ) TO {_sql_string(csv_path)} (
            FORMAT CSV,
            HEADER FALSE,
            DELIMITER ',',
            NULL '\\N'
        )
        """,
        [parquet_path],
    )

    with open(csv_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"COPY {SCHEMA}.fact_sensor_reading "
            "(sensor_id, product_id, department_id, create_at, product_expire) "
            "FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            f,
        )

    Path(csv_path).unlink(missing_ok=True)

    # COPY doesn't update cursor.rowcount; count rows for this day
    start_ts = f"{day} 00:00:00"
    end_ts = f"{day} 23:59:59"
    cursor.execute(
        f"SELECT count(*) FROM {SCHEMA}.fact_sensor_reading "
        "WHERE create_at >= %s AND create_at <= %s",
        (start_ts, end_ts),
    )
    return cursor.fetchone()[0]


def _truncate_for_full_reload(cursor) -> None:
    if TRUNCATE_DIMENSIONS_ON_FULL_RELOAD:
        print("Full reload: truncating fact and dimension tables.")
        cursor.execute(
            f"""
            TRUNCATE TABLE
                {SCHEMA}.fact_sensor_reading,
                {SCHEMA}.dim_sensor,
                {SCHEMA}.dim_product,
                {SCHEMA}.dim_department
            RESTART IDENTITY CASCADE
            """
        )
    else:
        print("Full reload: truncating fact table only.")
        cursor.execute(
            f"TRUNCATE TABLE {SCHEMA}.fact_sensor_reading RESTART IDENTITY"
        )


def _drop_fact_indexes(cursor) -> None:
    for index_name, _ in FACT_INDEXES:
        cursor.execute(f"DROP INDEX IF EXISTS {SCHEMA}.{index_name}")


def _create_fact_indexes(cursor) -> None:
    t0 = time.time()
    cursor.execute(
        f"SET LOCAL maintenance_work_mem = '{INDEX_MAINTENANCE_WORK_MEM}'"
    )
    cursor.execute(
        f"SET LOCAL max_parallel_maintenance_workers = {INDEX_MAX_PARALLEL_WORKERS}"
    )
    for index_name, create_sql in FACT_INDEXES:
        t_idx = time.time()
        cursor.execute(create_sql)
        print(f"  Index {index_name}: created in {time.time() - t_idx:.1f}s")
    print(f"All indexes created in {time.time() - t0:.1f}s")



def _analyze_tables(cursor) -> None:
    for table_name in (
        "dim_department",
        "dim_product",
        "dim_sensor",
        "fact_sensor_reading",
    ):
        cursor.execute(f"ANALYZE {SCHEMA}.{table_name}")


def _validate_counts(cursor, data: dict, loaded_rows: int) -> None:
    expected_rows = int(data.get("expected_rows") or 0)

    if FULL_RELOAD:
        cursor.execute(f"SELECT count(*) FROM {SCHEMA}.fact_sensor_reading")
        fact_rows = int(cursor.fetchone()[0])
        if fact_rows != expected_rows:
            raise ValueError(
                f"Validation failed: source has {expected_rows:,} valid rows, "
                f"Postgres fact has {fact_rows:,} rows."
            )
    elif loaded_rows != expected_rows:
        raise ValueError(
            f"Validation failed: expected to load {expected_rows:,} rows, "
            f"loaded {loaded_rows:,} rows."
        )

    if VALIDATE_PER_DAY_COUNTS:
        for item in data.get("daily_files", []):
            day = item["day"]
            day_dt = datetime.strptime(day, "%Y-%m-%d")
            next_day = day_dt + timedelta(days=1)
            cursor.execute(
                f"""
                SELECT count(*)
                FROM {SCHEMA}.fact_sensor_reading
                WHERE create_at >= %s AND create_at < %s
                """,
                (day_dt, next_day),
            )
            target_count = int(cursor.fetchone()[0])
            expected_day_count = int(item.get("row_count") or 0)
            if target_count != expected_day_count:
                raise ValueError(
                    f"Validation failed for {day}: source has "
                    f"{expected_day_count:,}, Postgres has {target_count:,}."
                )

    cursor.execute(
        f"""
        SELECT
            (SELECT count(*) FROM {SCHEMA}.dim_department),
            (SELECT count(*) FROM {SCHEMA}.dim_product),
            (SELECT count(*) FROM {SCHEMA}.dim_sensor),
            (SELECT count(*) FROM {SCHEMA}.fact_sensor_reading)
        """
    )
    dept_count, product_count, sensor_count, fact_count = cursor.fetchone()
    print(
        "Validation passed: "
        f"facts={fact_count:,}, departments={dept_count:,}, "
        f"products={product_count:,}, sensors={sensor_count:,}"
    )


@data_exporter
def load_to_postgres(data: dict, **kwargs) -> dict:
    """
    Fast KISS loader — DuckDB FK resolution + direct COPY.
    
    1. Collect all dimension values → upsert once
    2. Fetch dim ID maps → register in DuckDB
    3. Per day: DuckDB JOIN + native CSV → Postgres COPY directly into fact
    4. No staging table, no Postgres JOINs, no Python csv.writer
    """
    daily_files = data.get("daily_files", [])
    expected_rows = int(data.get("expected_rows") or 0)

    if not daily_files:
        print("No prepared daily files to load.")
        return {"loaded_rows": 0, "expected_rows": expected_rows, "days": 0}

    _apply_schema_sql_if_requested()

    csv_dir = "/tmp/sensor_pipeline_csv"
    Path(csv_dir).mkdir(parents=True, exist_ok=True)

    loaded_rows = 0
    started_at = time.time()
    raw_conn = None
    cursor = None
    duck_con = duckdb.connect()
    _configure_duckdb(duck_con)

    try:
        raw_conn = engine.raw_connection()
        cursor = raw_conn.cursor()
        cursor.execute("SET datestyle = 'ISO, YMD'")
        cursor.execute("SET synchronous_commit = off")
        cursor.execute(f"SET search_path TO {SCHEMA}")

        if FULL_RELOAD:
            _truncate_for_full_reload(cursor)
        raw_conn.commit()

        if MANAGE_FACT_INDEXES and FULL_RELOAD:
            print("Dropping fact indexes before full reload bulk load.")
            _drop_fact_indexes(cursor)
            raw_conn.commit()

        # --- Phase 1: Collect & upsert all dimensions once ---
        _collect_and_upsert_all_dimensions(duck_con, daily_files, cursor)
        raw_conn.commit()

        # --- Phase 2: Fetch dim maps, register in DuckDB ---
        dept_rows, prod_rows, sensor_rows = _fetch_dimension_maps(cursor)
        _register_dims_in_duckdb(duck_con, dept_rows, prod_rows, sensor_rows)

        # --- Phase 3: Per-day idempotent delete (incremental mode) ---
        if not FULL_RELOAD:
            for item in daily_files:
                day = item["day"]
                day_dt = datetime.strptime(day, "%Y-%m-%d")
                next_day = day_dt + timedelta(days=1)
                cursor.execute(
                    f"DELETE FROM {SCHEMA}.fact_sensor_reading "
                    "WHERE create_at >= %s AND create_at < %s",
                    (day_dt, next_day),
                )
            raw_conn.commit()

        # --- Phase 4: DuckDB JOIN + direct COPY per day ---
        for index, item in enumerate(daily_files, start=1):
            day = item["day"]
            load_path = item["load_path"]
            t_day = time.time()

            inserted = _duckdb_resolve_and_copy(
                duck_con, load_path, cursor, csv_dir, day
            )
            raw_conn.commit()
            loaded_rows += inserted

            print(
                f"Day {index}/{len(daily_files)} {day}: "
                f"{inserted:,} rows in {time.time() - t_day:.1f}s"
            )

        if MANAGE_FACT_INDEXES and FULL_RELOAD:
            print("Recreating fact indexes after full reload bulk load.")
            _create_fact_indexes(cursor)
            raw_conn.commit()

        print("Analyzing loaded tables.")
        _analyze_tables(cursor)
        raw_conn.commit()

        _validate_counts(cursor, data, loaded_rows)
        raw_conn.commit()

    finally:
        duck_con.close()
        if cursor:
            cursor.close()
        if raw_conn:
            raw_conn.close()

    elapsed = time.time() - started_at
    print(
        f"Loaded {loaded_rows:,}/{expected_rows:,} rows "
        f"across {len(daily_files)} day(s) in {elapsed / 60:.1f} minutes."
    )

    return {
        "loaded_rows": loaded_rows,
        "expected_rows": expected_rows,
        "days": len(daily_files),
        "elapsed_seconds": elapsed,
        "load_mode": LOAD_MODE,
    }
