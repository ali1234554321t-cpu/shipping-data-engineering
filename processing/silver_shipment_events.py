
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "bronze"
    / "shipment_events"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "silver"
    / "shipment_events"
)


def main():
    spark = (
        SparkSession.builder
        .appName("ShippingSilverShipmentEvents")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(str(INPUT_DIR))
        input_count = df.count()

        cleaned = (
            df
            .withColumn(
                "shipment_id",
                F.trim(F.col("shipment_id"))
            )
            .withColumn(
                "status",
                F.trim(F.col("status"))
            )
            .withColumn(
                "location",
                F.trim(F.col("location"))
            )
            .withColumn(
                "event_timestamp",
                F.to_timestamp(F.col("event_time"))
            )
        )

        valid = cleaned.filter(
            F.col("shipment_id").isNotNull()
            & (F.col("shipment_id") != "")
            & F.col("status").isNotNull()
            & (F.col("status") != "")
            & F.col("location").isNotNull()
            & (F.col("location") != "")
            & F.col("event_timestamp").isNotNull()
        )

        # Remove exact duplicate events, but preserve different
        # status updates for the same shipment.
        valid = valid.dropDuplicates([
            "shipment_id",
            "status",
            "location",
            "event_timestamp",
        ])

        result = valid.select(
            "shipment_id",
            "status",
            "location",
            "event_timestamp",
            "bronze_loaded_at",
        )

        output_count = result.count()

        import shutil
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        (
            result.write
            .mode("errorifexists")
            .parquet(str(OUTPUT_DIR))
        )

        print(f"Bronze input rows: {input_count}")
        print(f"Silver valid rows: {output_count}")
        print(f"Rows removed: {input_count - output_count}")
        print(f"Silver output: {OUTPUT_DIR}")

        result.orderBy("event_timestamp").show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()