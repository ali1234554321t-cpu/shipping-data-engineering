
import os
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession


PROJECT_DIR = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_DIR / "data/medallion/gold/shipment_summary"

DB_CONFIG = {
    "host": os.getenv("SHIPPING_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("SHIPPING_DB_PORT", "5433")),
    "dbname": os.getenv("SHIPPING_DB_NAME", "shipping_db"),
    "user": os.getenv("SHIPPING_DB_USER", "shipping_user"),
    "password": os.environ["SHIPPING_DB_PASSWORD"],
}

TABLE_NAME = "gold_shipment_summary"


def main():
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Gold data directory not found: {PARQUET_PATH}"
        )

    spark = (
        SparkSession.builder
        .appName("LoadGoldShipmentSummary")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(PARQUET_PATH))

        columns = [
            "summary_type",
            "status",
            "location",
            "shipment_count",
        ]

        rows = [
            tuple(row)
            for row in df.select(*columns).toLocalIterator()
        ]

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS gold_shipment_summary (
                summary_type VARCHAR(50),
                status VARCHAR(100),
                location VARCHAR(100),
                shipment_count BIGINT
            )
        """

        insert_sql = """
            INSERT INTO gold_shipment_summary (
                summary_type,
                status,
                location,
                shipment_count
            )
            VALUES (%s, %s, %s, %s)
        """

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)

                # Replace the old snapshot transactionally.
                cur.execute(f"DELETE FROM {TABLE_NAME}")

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
        print("Shipment Gold load completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()