import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

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

# Step 2: Connect to target DB
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

# Step 3: Create table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices_2 (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        stock_symbol VARCHAR(10) NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        volume INTEGER NOT NULL,
        bid_price NUMERIC(10, 2) NOT NULL,
        ask_price NUMERIC(10, 2) NOT NULL,
        spread NUMERIC(10, 2) NOT NULL
    );
""")
conn.commit()

# Step 4: Ingest each CSV
for filename in os.listdir(CSV_FOLDER_PATH):
    if filename.endswith(".csv"):
        csv_path = os.path.join(CSV_FOLDER_PATH, filename)
        print(f"Ingesting: {csv_path}")
        with open(csv_path, 'r') as file:
            next(file)  # Skip header
            cursor.copy_expert(
                """
                COPY stock_prices_2 (timestamp, stock_symbol, price, volume, bid_price, ask_price, spread)
                FROM STDIN WITH CSV
                """,
                file
            )
        conn.commit()
        print(f"✔ Done ingesting {filename}")

cursor.close()
conn.close()
print(" All CSVs imported into stock_prices_2.")
