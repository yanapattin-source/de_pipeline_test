import os
from pathlib import Path

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_parquet_batch(**kwargs) -> dict:
    """
    KISS non-dynamic loader.

    Returns one manifest object, so Mage runs the downstream transform only once.
    It does not read the parquet data here. Reading 82M rows in a loader would
    waste memory and make Mage pass a huge object between blocks.
    """
    data_dir = os.environ.get("DATA_DIR", "/home/src/data_sample")
    data_path = Path(data_dir)

    if not data_path.exists():
        raise FileNotFoundError(f"DATA_DIR does not exist: {data_dir}")

    parquet_files = sorted(p for p in data_path.iterdir() if p.suffix == ".parquet")
    days = sorted({p.name[:10] for p in parquet_files})

    manifest = {
        "source_dir": data_dir,
        "days": days,
        "source_file_count": len(parquet_files),
        "prepared_dir": os.environ.get(
            "PREPARED_DATA_DIR",
            "/tmp/sensor_pipeline_daily_parquet",
        ),
        "compact_daily_parquet": os.environ.get(
            "COMPACT_DAILY_PARQUET",
            "true",
        ).lower() in ("1", "true", "yes"),
        "load_mode": os.environ.get("LOAD_MODE", "full_reload"),
    }

    if not days:
        print(f"No parquet files found in {data_dir}")
        return manifest

    print(
        "Found "
        f"{len(parquet_files):,} parquet files across {len(days)} day(s): "
        f"{days[0]} to {days[-1]}"
    )
    print("Non-dynamic mode: downstream transform/exporter will run once each.")
    return manifest
