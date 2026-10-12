import pandas as pd
import psycopg2

CSV_PATH = "data/processed/port_capacity_clean.csv"

columns = ["id", "date", "year", "indicator", "value1", "units", "note", "source"]
df = pd.read_csv(CSV_PATH, usecols=columns)

df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
df["value1"] = pd.to_numeric(df["value1"], errors="coerce")
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df = df.dropna(subset=["id", "date", "value1", "indicator"])

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
                CREATE TABLE IF NOT EXISTS port_capacity (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    source_id VARCHAR(100) UNIQUE,
                    observation_date DATE NOT NULL,
                    year INTEGER,
                    indicator TEXT NOT NULL,
                    value NUMERIC,
                    units TEXT,
                    note TEXT,
                    source TEXT
                );
            """)

            insert_sql = """
                INSERT INTO port_capacity
                    (source_id, observation_date, year, indicator,
                     value, units, note, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id) DO NOTHING;
            """

            rows = [
                (
                    str(row["id"]),
                    row["date"],
                    int(row["year"]) if pd.notna(row["year"]) else None,
                    row["indicator"],
                    float(row["value1"]),
                    row["units"] if pd.notna(row["units"]) else None,
                    row["note"] if pd.notna(row["note"]) else None,
                    row["source"] if pd.notna(row["source"]) else None,
                )
                for _, row in df.iterrows()
            ]

            cursor.executemany(insert_sql, rows)
            cursor.execute("SELECT COUNT(*) FROM port_capacity;")
            total = cursor.fetchone()[0]

    print(f"CSV rows prepared: {len(df)}")
    print(f"Total rows in PostgreSQL: {total}")

finally:
    connection.close()
