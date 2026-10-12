import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

password = os.getenv("POSTGRES_PASSWORD")
if not password:
    raise RuntimeError("POSTGRES_PASSWORD is missing from .env")

try:
    connection = psycopg2.connect(
        host="localhost",
        port=5433,
        database="shipping_db",
        user="shipping_user",
        password=password,
    )

    print("Connected to PostgreSQL successfully!")
    connection.close()

except Exception:
    print("Connection failed. Check your database settings.")
