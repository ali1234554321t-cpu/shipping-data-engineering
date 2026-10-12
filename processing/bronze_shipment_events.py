
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "shipment_events.jsonl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "medallion" / "bronze" / "shipment_events"

schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location", StringType(), True),
    StructField("event_time", StringType(), True),
])

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Shipment events file not found: {INPUT_FILE}"
        )

    spark = (
        SparkSession.builder
        .appName("ShippingBronzeShipmentEvents")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        records = []

        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON on line {line_number}"
                    ) from exc

                records.append({
                    "shipment_id": record.get("shipment_id"),
                    "status": record.get("status"),
                    "location": record.get("location"),
                    "event_time": record.get("event_time"),
                })

        if not records:
            raise ValueError("No shipment events found in input file.")

        df = spark.createDataFrame(records, schema=schema)

        loaded_at = datetime.now(timezone.utc).isoformat()
        df = df.withColumn(
            "bronze_loaded_at",
            __import__("pyspark.sql.functions", fromlist=["lit"]).lit(loaded_at)
        )

        import shutil

        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        df.write.mode("errorifexists").parquet(str(OUTPUT_DIR))

        print(f"Input events: {len(records)}")
        print(f"Bronze output: {OUTPUT_DIR}")
        df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()