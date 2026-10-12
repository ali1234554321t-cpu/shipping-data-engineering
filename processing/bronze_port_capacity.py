
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "port_capacity.json"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "bronze"
    / "port_capacity"
)


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"BTS source file not found: {INPUT_FILE}"
        )

    spark = (
        SparkSession.builder
        .appName("ShippingBronzePortCapacity")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read the original BTS JSON records without
        # applying Silver-layer cleaning rules.
        df = (
            spark.read
            .option("multiLine", True)
            .json(str(INPUT_FILE))
        )

        input_count = df.count()

        # Record when this Bronze batch was loaded.
        loaded_at = datetime.now(timezone.utc).isoformat()

        bronze_df = df.withColumn(
            "bronze_loaded_at",
            F.lit(loaded_at)
        )

        import shutil

        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        (
            bronze_df.write
            .mode("errorifexists")
            .parquet(str(OUTPUT_DIR))
        )

        print(f"BTS input records: {input_count}")
        print(f"Bronze output records: {bronze_df.count()}")
        print(f"Bronze columns: {len(bronze_df.columns)}")
        print(f"Bronze output: {OUTPUT_DIR}")

        print("\nOriginal BTS fields and sample records:")
        bronze_df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()