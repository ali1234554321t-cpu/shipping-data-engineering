
import os
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession


PROJECT_DIR = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_DIR / "data/medallion/gold/port_capacity_summary"

DB_CONFIG = {
    "host": os.getenv("SHIPPING_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("SHIPPING_DB_PORT", "5433")),
    "dbname": os.getenv("SHIPPING_DB_NAME", "shipping_db"),
    "user": os.getenv("SHIPPING_DB_USER", "shipping_user"),
    "password": os.environ["SHIPPING_DB_PASSWORD"],
}

TABLE_NAME = "gold_port_capacity_summary"


def main():
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Gold data directory not found: {PARQUET_PATH}"
        )

    spark = (
        SparkSession.builder
        .appName("LoadGoldPortCapacitySummary")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(PARQUET_PATH))

        columns = [
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
            "summary_type",
        ]

        rows = [
            tuple(row)
            for row in df.select(*columns).toLocalIterator()
        ]

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS gold_port_capacity_summary (
                year INTEGER,
                month DATE,
                indicator_category TEXT,
                indicator TEXT,
                units TEXT,
                measure1 TEXT,
                measure1_description TEXT,
                measure2 TEXT,
                measure2_description TEXT,
                record_count BIGINT,
                observation_days BIGINT,
                min_value NUMERIC(20, 4),
                max_value NUMERIC(20, 4),
                avg_value NUMERIC(24, 8),
                summary_type VARCHAR(20)
            )
        """

        insert_sql = """
            INSERT INTO gold_port_capacity_summary (
                year, month, indicator_category, indicator, units,
                measure1, measure1_description, measure2,
                measure2_description, record_count, observation_days,
                min_value, max_value, avg_value, summary_type
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)

                # Replace the old snapshot inside the same transaction.
                cur.execute(
                    f"DELETE FROM {TABLE_NAME}"
                )

                if rows:
                    cur.executemany(insert_sql, rows)

                cur.execute(
                    f"SELECT COUNT(*) FROM {TABLE_NAME}"
                )
                loaded_count = cur.fetchone()[0]

                if loaded_count != len(rows):
                    raise RuntimeError(
                        f"Row count mismatch: expected {len(rows)}, "
                        f"found {loaded_count}"
                    )

        print(f"Rows read from Gold: {len(rows)}")
        print(f"Rows in PostgreSQL: {loaded_count}")
        print("Port capacity Gold load completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()