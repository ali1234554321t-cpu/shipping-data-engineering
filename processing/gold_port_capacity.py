
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "silver"
    / "port_capacity"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "gold"
    / "port_capacity_summary"
)


def main():
    if not INPUT_DIR.exists():
        raise FileNotFoundError(
            f"Silver data not found: {INPUT_DIR}"
        )

    spark = (
        SparkSession.builder
        .appName("ShippingGoldPortCapacity")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver = spark.read.parquet(str(INPUT_DIR))

        # Classify indicators without mixing their units.
        # Order matters: check specific categories before broad ones.
        categorized = silver.withColumn(
            "indicator_category",
            F.when(
                F.col("indicator").contains("Containerships")
                & (
                    F.col("indicator").contains("Awaiting Berths")
                    | F.col("indicator").contains("Anchored off")
                ),
                F.lit("Port Congestion"),
            )
            .when(
                F.col("indicator").contains("TEUs"),
                F.lit("Container Volume"),
            )
            .when(
                F.col("indicator").contains("Truck"),
                F.lit("Road Freight"),
            )
            .when(
                F.col("indicator").contains("Dwell Time"),
                F.lit("Rail Dwell Time"),
            )
            .when(
                F.col("indicator").contains("Rail")
                | F.col("indicator").contains("Railroad"),
                F.lit("Rail Transport"),
            )
            .otherwise(F.lit("Other Logistics")),
        )

        # Summary 1: annual metrics, grouped by indicator,
        # unit, and measurement dimensions.
        annual_summary = (
            categorized
            .groupBy(
                "year",
                "indicator_category",
                "indicator",
                "units",
                "measure1",
                "measure1_description",
                "measure2",
                "measure2_description",
            )
            .agg(
                F.count("*").alias("record_count"),
                F.countDistinct("observation_date").alias(
                    "observation_days"
                ),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.avg("value").alias("avg_value"),
            )
            .withColumn("summary_type", F.lit("annual"))
        )

        # Summary 2: monthly metrics, keeping the same
        # indicator, unit, and measurement dimensions.
        monthly_summary = (
            categorized
            .withColumn(
                "month",
                F.date_trunc(
                    "month", F.col("observation_date")
                ).cast("date"),
            )
            .groupBy(
                "month",
                "indicator_category",
                "indicator",
                "units",
                "measure1",
                "measure1_description",
                "measure2",
                "measure2_description",
            )
            .agg(
                F.count("*").alias("record_count"),
                F.countDistinct("observation_date").alias(
                    "observation_days"
                ),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.avg("value").alias("avg_value"),
            )
            .withColumn("summary_type", F.lit("monthly"))
            .withColumn("year", F.year("month"))
        )

        # Use one consistent schema for both summary grains.
        summary_columns = [
            "summary_type",
            "year",
            "month",
            "indicator_category",
            "indicator",
            "units",
            "measure1",
            "measure1_description",
            "measure2",
            "measure2_description",
            "record_count",
            "observation_days",
            "min_value",
            "max_value",
            "avg_value",
        ]

        annual_output = (
            annual_summary
            .withColumn("month", F.lit(None).cast("date"))
            .select(*summary_columns)
        )

        monthly_output = monthly_summary.select(*summary_columns)

        gold = annual_output.unionByName(monthly_output)

        import shutil
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        (
            gold.write
            .mode("errorifexists")
            .partitionBy("summary_type")
            .parquet(str(OUTPUT_DIR))
        )

        print(f"Silver input rows: {silver.count()}")
        print(f"Gold summary rows: {gold.count()}")
        print(
            "Distinct indicators:",
            silver.select("indicator").distinct().count(),
        )
        print(f"Gold output: {OUTPUT_DIR}")

        print("\nAnnual summary sample:")
        (
            gold.filter(F.col("summary_type") == "annual")
            .orderBy("year", "indicator")
            .select(
                "year",
                "indicator_category",
                "indicator",
                "units",
                "record_count",
                "avg_value",
            )
            .show(10, truncate=False)
        )

        print("\nMonthly summary sample:")
        (
            gold.filter(F.col("summary_type") == "monthly")
            .orderBy(F.col("month").desc(), "indicator")
            .select(
                "month",
                "indicator_category",
                "indicator",
                "units",
                "record_count",
                "avg_value",
            )
            .show(10, truncate=False)
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()