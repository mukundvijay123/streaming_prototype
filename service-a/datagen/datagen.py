import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv
import csv
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
CSV_FOLDER_PATH = os.getenv("CSV_FOLDER_PATH")  # Folder containing CSVs

# Step 1: Connect to default DB and create target DB if needed
conn = psycopg2.connect(
    dbname="postgres",
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (DATABASE,))
if not cursor.fetchone():
    cursor.execute(f"CREATE DATABASE {DATABASE};")
    print(f"Database '{DATABASE}' created.")
else:
    print(f"Database '{DATABASE}' already exists.")

cursor.close()
conn.close()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

# Step 1: Create stock_prices_3 table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices_4 (
        timestamp TIMESTAMPTZ NOT NULL,
        stock_symbol VARCHAR(10) NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        volume INTEGER NOT NULL,
        bid_price NUMERIC(10, 2) NOT NULL,
        ask_price NUMERIC(10, 2) NOT NULL,
        spread NUMERIC(10, 2) NOT NULL
    );
""")
conn.commit()

# Step 2: Process CSVs and insert into DB
for filename in os.listdir(CSV_FOLDER_PATH):
    if filename.endswith(".csv"):
        csv_path = os.path.join(CSV_FOLDER_PATH, filename)
        print(f"📥 Ingesting: {csv_path}")

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert timestamp to ISO format with Z
                dt = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
                dt_utc = dt.replace(tzinfo=timezone.utc)
                iso_timestamp = dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                cursor.execute("""
                    INSERT INTO stock_prices_4
                    (timestamp, stock_symbol, price, volume, bid_price, ask_price, spread)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    iso_timestamp,
                    row['stock'],
                    row['price'],
                    row['volume'],
                    row['bid_price'],
                    row['ask_price'],
                    row['spread']
                ))
        conn.commit()
        print(f"✔ Done ingesting {filename}")

cursor.close()
conn.close()
print("✅ All CSVs imported into stock_prices_4.")
