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
LOAD_MODE = os.environ.get("LOAD_MODE", "full_reload").lower()
FULL_RELOAD = LOAD_MODE in ("full_reload", "full", "truncate")
MANAGE_FACT_INDEXES = os.environ.get("MANAGE_FACT_INDEXES", "true").lower() in (
    "1",
    "true",
    "yes",
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

APPLY_SCHEMA_SQL = os.environ.get("APPLY_SCHEMA_SQL", "false").lower() in (
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
    """Optional helper for non-Docker runs. Docker compose initializes schema.sql."""
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


def _export_parquet_to_csv(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    csv_path: str,
) -> None:
    Path(csv_path).unlink(missing_ok=True)
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
        ) TO {_sql_string(csv_path)} (
            FORMAT CSV,
            HEADER FALSE,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL '\\N'
        )
        """,
        [parquet_path],
    )


def _create_temp_stage(cursor) -> None:
    cursor.execute(
        """
        CREATE TEMP TABLE stg_sensor_reading (
            department_name VARCHAR(32) NOT NULL,
            sensor_serial VARCHAR(64) NOT NULL,
            product_name VARCHAR(16) NOT NULL,
            create_at TIMESTAMP NOT NULL,
            product_expire TIMESTAMP NOT NULL
        ) ON COMMIT PRESERVE ROWS
        """
    )


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


def _delete_existing_day(cursor, day: str) -> int:
    day_dt = datetime.strptime(day, "%Y-%m-%d")
    next_day = day_dt + timedelta(days=1)
    cursor.execute(
        f"""
        DELETE FROM {SCHEMA}.fact_sensor_reading
        WHERE create_at >= %s AND create_at < %s
        """,
        (day_dt, next_day),
    )
    return cursor.rowcount


def _drop_fact_indexes(cursor) -> None:
    for index_name, _ in FACT_INDEXES:
        cursor.execute(f"DROP INDEX IF EXISTS {SCHEMA}.{index_name}")


def _create_fact_indexes(cursor) -> None:
    for _, create_sql in FACT_INDEXES:
        cursor.execute(create_sql)


def _copy_csv_to_stage(cursor, csv_path: str) -> None:
    with open(csv_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            """
            COPY stg_sensor_reading
                (department_name, sensor_serial, product_name, create_at, product_expire)
            FROM STDIN WITH (FORMAT CSV, NULL '\\N')
            """,
            f,
        )


def _load_stage_to_star_schema(cursor) -> int:
    cursor.execute("SELECT count(*) FROM stg_sensor_reading")
    stage_count = int(cursor.fetchone()[0])

    if stage_count == 0:
        return 0

    cursor.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_department (department_name)
        SELECT DISTINCT department_name
        FROM stg_sensor_reading
        ON CONFLICT (department_name) DO NOTHING
        """
    )

    cursor.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_product (product_name)
        SELECT DISTINCT product_name
        FROM stg_sensor_reading
        ON CONFLICT (product_name) DO NOTHING
        """
    )

    cursor.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_sensor (sensor_serial, department_id)
        SELECT DISTINCT
            stg.sensor_serial,
            dept.department_id
        FROM stg_sensor_reading AS stg
        JOIN {SCHEMA}.dim_department AS dept
            ON dept.department_name = stg.department_name
        ON CONFLICT (sensor_serial, department_id) DO NOTHING
        """
    )

    cursor.execute(
        f"""
        INSERT INTO {SCHEMA}.fact_sensor_reading
            (sensor_id, product_id, department_id, create_at, product_expire)
        SELECT
            sensor.sensor_id,
            product.product_id,
            dept.department_id,
            stg.create_at,
            stg.product_expire
        FROM stg_sensor_reading AS stg
        JOIN {SCHEMA}.dim_department AS dept
            ON dept.department_name = stg.department_name
        JOIN {SCHEMA}.dim_product AS product
            ON product.product_name = stg.product_name
        JOIN {SCHEMA}.dim_sensor AS sensor
            ON sensor.sensor_serial = stg.sensor_serial
           AND sensor.department_id = dept.department_id
        """
    )
    inserted_count = cursor.rowcount

    if inserted_count != stage_count:
        raise ValueError(
            f"Fact insert mismatch: staged {stage_count:,}, inserted {inserted_count:,}. "
            "This means dimension join accuracy failed."
        )

    return inserted_count


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
    Fast KISS loader.

    One Mage exporter run:
    - uses DuckDB only to convert parquet to daily CSV files
    - COPYs CSV into a Postgres TEMP table
    - upserts dimensions with SQL
    - inserts facts with SQL joins
    - validates source counts against Postgres counts

    No permanent staging table and no giant pandas DataFrame.
    """
    daily_files = data.get("daily_files", [])
    expected_rows = int(data.get("expected_rows") or 0)

    if not daily_files:
        print("No prepared daily files to load.")
        return {"loaded_rows": 0, "expected_rows": expected_rows, "days": 0}

    _apply_schema_sql_if_requested()

    tmp_dir = Path(os.environ.get("POSTGRES_LOAD_TMP_DIR", "/tmp/sensor_pipeline_csv"))
    tmp_dir.mkdir(parents=True, exist_ok=True)

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

        if MANAGE_FACT_INDEXES:
            print("Dropping fact indexes before bulk load.")
            _drop_fact_indexes(cursor)
            raw_conn.commit()

        _create_temp_stage(cursor)
        raw_conn.commit()

        for index, item in enumerate(daily_files, start=1):
            day = item["day"]
            load_path = item["load_path"]
            csv_path = str(tmp_dir / f"sensor_{day}.csv")
            t0 = time.time()

            try:
                cursor.execute("TRUNCATE stg_sensor_reading")

                if not FULL_RELOAD:
                    deleted = _delete_existing_day(cursor, day)
                    if deleted:
                        print(f"{day}: deleted {deleted:,} existing fact rows")

                _export_parquet_to_csv(duck_con, load_path, csv_path)
                _copy_csv_to_stage(cursor, csv_path)
                inserted = _load_stage_to_star_schema(cursor)
                raw_conn.commit()

                loaded_rows += inserted
                print(
                    f"Loaded {index}/{len(daily_files)} {day}: "
                    f"{inserted:,} rows in {time.time() - t0:.1f}s"
                )
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                Path(csv_path).unlink(missing_ok=True)

        if MANAGE_FACT_INDEXES:
            print("Recreating fact indexes after bulk load.")
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
        f"Loaded {loaded_rows:,}/{expected_rows:,} expected rows "
        f"across {len(daily_files)} day(s) in {elapsed / 60:.1f} minutes."
    )

    return {
        "loaded_rows": loaded_rows,
        "expected_rows": expected_rows,
        "days": len(daily_files),
        "elapsed_seconds": elapsed,
        "load_mode": LOAD_MODE,
    }
