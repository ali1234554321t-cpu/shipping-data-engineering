
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "bronze"
    / "port_capacity"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "medallion"
    / "silver"
    / "port_capacity"
)


def main():
    if not INPUT_DIR.exists():
        raise FileNotFoundError(
            f"Bronze data not found: {INPUT_DIR}"
        )

    spark = (
        SparkSession.builder
        .appName("ShippingSilverPortCapacity")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze = spark.read.parquet(str(INPUT_DIR))
        input_count = bronze.count()

        # Rename source fields to analytics-friendly names.
        df = bronze.select(
            F.col(":id").cast("string").alias("source_id"),
            F.col(":version").cast("string").alias("source_version"),
            F.col(":created_at").cast("string").alias("source_created_at"),
            F.col(":updated_at").cast("string").alias("source_updated_at"),
            F.col("id").cast("string").alias("source_record_id"),
            F.col("date").cast("string").alias("date_raw"),
            F.col("year").cast("string").alias("year_raw"),
            F.col("indicator").cast("string").alias("indicator"),
            F.col("value1").cast("string").alias("value_raw"),
            F.col("units").cast("string").alias("units"),
            F.col("note").cast("string").alias("note"),
            F.col("source").cast("string").alias("source"),
            F.col("measure1").cast("string").alias("measure1"),
            F.col("measure1_description").cast("string").alias(
                "measure1_description"
            ),
            F.col("measure2").cast("string").alias("measure2"),
            F.col("measure2_description").cast("string").alias(
                "measure2_description"
            ),
            F.col("bronze_loaded_at").cast("string").alias(
                "bronze_loaded_at"
            ),
        )

        # Parse dates and numeric values safely.
        cleaned = (
            df
            .withColumn(
                "observation_date",
                F.to_date(
                    F.substring(F.col("date_raw"), 1, 10),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "year",
                F.col("year_raw").cast("int"),
            )
            .withColumn(
                "value",
                F.expr("try_cast(value_raw AS DECIMAL(20, 4))"),
            )
            .withColumn(
                "source_created_at",
                F.to_timestamp("source_created_at"),
            )
            .withColumn(
                "source_updated_at",
                F.to_timestamp("source_updated_at"),
            )
            .withColumn(
                "indicator",
                F.trim(F.col("indicator")),
            )
            .withColumn(
                "units",
                F.trim(F.col("units")),
            )
            .withColumn(
                "source_id",
                F.trim(F.col("source_id")),
            )
        )

        # Report data-quality issues before excluding any records.
        invalid_id = (
            F.col("source_id").isNull()
            | (F.col("source_id") == "")
        )
        invalid_date = F.col("observation_date").isNull()
        invalid_year = F.col("year").isNull()
        invalid_indicator = (
            F.col("indicator").isNull()
            | (F.col("indicator") == "")
        )
        invalid_value = F.col("value").isNull()
        invalid_units = (
            F.col("units").isNull()
            | (F.col("units") == "")
        )

        quality_counts = cleaned.agg(
            F.sum(F.when(invalid_id, 1).otherwise(0)).alias(
                "invalid_ids"
            ),
            F.sum(F.when(invalid_date, 1).otherwise(0)).alias(
                "invalid_dates"
            ),
            F.sum(F.when(invalid_year, 1).otherwise(0)).alias(
                "invalid_years"
            ),
            F.sum(F.when(invalid_indicator, 1).otherwise(0)).alias(
                "invalid_indicators"
            ),
            F.sum(F.when(invalid_value, 1).otherwise(0)).alias(
                "invalid_values"
            ),
            F.sum(F.when(invalid_units, 1).otherwise(0)).alias(
                "invalid_units"
            ),
        ).first()

        print(f"Bronze input rows: {input_count}")
        print("\nData-quality issues before filtering:")
        for name in quality_counts.asDict():
            print(f"{name}: {quality_counts[name] or 0}")

        # Keep only records that meet the required analytical fields.
        valid = cleaned.filter(
            ~invalid_id
            & ~invalid_date
            & ~invalid_year
            & ~invalid_indicator
            & ~invalid_value
            & ~invalid_units
        )

        # Remove duplicate source IDs, retaining one record per ID.
        # Original records remain unchanged in Bronze.
        deduplicated = valid.dropDuplicates(["source_id"])

        output_count = deduplicated.count()
        removed_count = input_count - output_count

        # Preserve raw date/value strings for auditability.
        silver = deduplicated.select(
            "source_id",
            "source_version",
            "source_created_at",
            "source_updated_at",
            "source_record_id",
            "observation_date",
            "year",
            "indicator",
            "value",
            "units",
            "note",
            "source",
            "measure1",
            "measure1_description",
            "measure2",
            "measure2_description",
            "date_raw",
            "year_raw",
            "value_raw",
            "bronze_loaded_at",
        )

        import shutil
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        (
            silver.write
            .mode("errorifexists")
            .parquet(str(OUTPUT_DIR))
        )

        print(f"\nSilver output rows: {output_count}")
        print(f"Rows excluded or deduplicated: {removed_count}")
        print(f"Distinct indicators: {silver.select('indicator').distinct().count()}")
        print(
            "Date range:",
            silver.agg(
                F.min("observation_date").alias("first_date"),
                F.max("observation_date").alias("last_date"),
            ).first().asDict(),
        )
        print(f"Silver output: {OUTPUT_DIR}")

        print("\nSilver sample:")
        silver.select(
            "source_id",
            "observation_date",
            "year",
            "indicator",
            "value",
            "units",
            "measure1",
            "measure2",
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()