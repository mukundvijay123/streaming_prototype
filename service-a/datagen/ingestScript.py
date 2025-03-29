import psycopg2

# Database connection details
DATABASE = "multidb"
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5432"
CSV_FILE_PATH = r"C:\Users\Eshaan Mathur\Downloads\streaming_prototype\stock_data2.csv"

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
