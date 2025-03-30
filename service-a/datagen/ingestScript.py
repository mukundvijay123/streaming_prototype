import psycopg2
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

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

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