
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "silver"
    / "shipment_events"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "gold"
    / "shipment_summary"
)


def main():
    spark = (
        SparkSession.builder
        .appName("ShippingGoldShipmentSummary")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        events = spark.read.parquet(str(INPUT_DIR))

        # Select the latest known event for each shipment.
        window_spec = Window.partitionBy("shipment_id").orderBy(
            F.col("event_timestamp").desc(),
            F.col("status").asc(),
            F.col("location").asc(),
        )

        latest_events = (
            events
            .withColumn(
                "row_num",
                F.row_number().over(window_spec)
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        # Summary 1: current shipment count by status.
        status_summary = (
            latest_events
            .groupBy("status")
            .agg(
                F.countDistinct("shipment_id").alias("shipment_count")
            )
            .withColumn("summary_type", F.lit("status"))
            .withColumn("location", F.lit(None).cast("string"))
            .select(
                "summary_type",
                "status",
                "location",
                "shipment_count",
            )
        )

        # Summary 2: current shipment count by location and status.
        location_summary = (
            latest_events
            .groupBy("location", "status")
            .agg(
                F.countDistinct("shipment_id").alias("shipment_count")
            )
            .withColumn("summary_type", F.lit("location_status"))
            .select(
                "summary_type",
                "status",
                "location",
                "shipment_count",
            )
        )

        gold_summary = status_summary.unionByName(location_summary)

        import shutil
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        (
            gold_summary.write
            .mode("errorifexists")
            .parquet(str(OUTPUT_DIR))
        )

        print(f"Silver event rows: {events.count()}")
        print(f"Latest shipment count: {latest_events.count()}")
        print(f"Gold output: {OUTPUT_DIR}")

        print("\nLatest event per shipment:")
        latest_events.orderBy("shipment_id").show(
            20, truncate=False
        )

        print("\nGold analytical summary:")
        gold_summary.orderBy(
            "summary_type", "status", "location"
        ).show(50, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()