import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details from .env
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")

# Connect to PostgreSQL server (without specifying a database)
conn = psycopg2.connect(
    dbname="postgres",  # Default database
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Check if the database exists and create it if it doesn't
cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE}';")
if not cursor.fetchone():
    cursor.execute(f"CREATE DATABASE {DATABASE};")
    print(f"Database '{DATABASE}' created successfully.")
else:
    print(f"Database '{DATABASE}' already exists.")

cursor.close()
conn.close()

# Connect to the newly created database
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

# Create table if it doesn't exist
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

# Load CSV into the database
with open(CSV_FILE_PATH, 'r') as file:
    next(file)  # Skip header
    cursor.copy_expert(
        "COPY stock_prices_2 (timestamp, stock_symbol, price, volume, bid_price, ask_price, spread) FROM STDIN WITH CSV",
        file
    )

conn.commit()
cursor.close()
conn.close()

print("CSV data successfully imported into stock_prices_2.")