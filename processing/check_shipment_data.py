import psycopg2

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="shipping_db",
    user="shipping_user",
    password=__import__("os").getenv("SHIPPING_DB_PASSWORD"),
)

try:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT status, COUNT(*) AS events_count
            FROM shipment_events
            GROUP BY status
            ORDER BY events_count DESC;
        """)

        print("Shipment events by status:")
        for status, count in cursor.fetchall():
            print(f"{status}: {count}")

finally:
    connection.close()
