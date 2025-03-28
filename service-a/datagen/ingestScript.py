import psycopg2
import random
from datetime import datetime, timedelta

# Database connection details
DATABASE = "multidb"
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5432"

# Establish database connection
conn = psycopg2.connect(
    dbname=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

# Define start time (9:00 AM market open)
start_time = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
num_seconds = 23400  # 6.5 hours of trading data (one second per row)
stock_symbol = "XYZ"

# Insert data into the database
insert_query = """
INSERT INTO stock_prices (timestamp, stock_symbol, price, volume, bid_price, ask_price, spread)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

batch_size = 1000  # Insert in batches for efficiency
batch_data = []

for i in range(num_seconds):
    timestamp = start_time + timedelta(seconds=i)
    price = round(random.uniform(100, 200), 2)
    volume = random.randint(100, 10000)
    bid_price = round(price - random.uniform(0.1, 1), 2)
    ask_price = round(price + random.uniform(0.1, 1), 2)
    spread = round(ask_price - bid_price, 2)

    batch_data.append((timestamp, stock_symbol, price, volume, bid_price, ask_price, spread))

    # Insert in batches to optimize performance
    if len(batch_data) >= batch_size:
        cursor.executemany(insert_query, batch_data)
        conn.commit()
        batch_data = []

# Insert any remaining records
if batch_data:
    cursor.executemany(insert_query, batch_data)
    conn.commit()

print("Inserted stock price data into the database.")

# Close connections
cursor.close()
conn.close()
