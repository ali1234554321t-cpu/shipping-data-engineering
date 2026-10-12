import pandas as pd
import psycopg2

CSV_PATH = "data/processed/shipment_events_clean.csv"

df = pd.read_csv(CSV_PATH)
df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="shipping_db",
    user="shipping_user",
    password=__import__("os").getenv("SHIPPING_DB_PASSWORD"),
)

try:
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shipment_events (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    shipment_id VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    location VARCHAR(100) NOT NULL,
                    event_time TIMESTAMPTZ NOT NULL,
                    UNIQUE (shipment_id, status, location, event_time)
                );
            """)

            insert_sql = """
                INSERT INTO shipment_events
                    (shipment_id, status, location, event_time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (shipment_id, status, location, event_time)
                DO NOTHING;
            """

            rows = [
                (r.shipment_id, r.status, r.location, r.event_time.to_pydatetime())
                for r in df.itertuples(index=False)
            ]

            cursor.executemany(insert_sql, rows)

            cursor.execute("SELECT COUNT(*) FROM shipment_events;")
            total = cursor.fetchone()[0]

    print(f"CSV rows read: {len(df)}")
    print(f"Total rows in PostgreSQL: {total}")

finally:
    connection.close()
