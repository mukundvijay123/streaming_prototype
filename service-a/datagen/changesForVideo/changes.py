import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="arrow_kafka",
    user="postgres",
    password="your_password_here",  # Replace with your actual password
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Drop if exists
cur.execute("DROP TABLE IF EXISTS stocks1;")
cur.execute("DROP TABLE IF EXISTS stocks2;")

# Create new tables
cur.execute("SELECT * INTO stocks1 FROM stock_prices_2;")
cur.execute("SELECT * INTO stocks2 FROM stock_prices_4;")

# Perform updates
updates = [
    ("stocks1", "ABC", "TSLA"),
    ("stocks1", "LMN", "AMZN"),
    ("stocks1", "XYZ", "HPE"),
    ("stocks2", "ABC", "TSLA"),
    ("stocks2", "LMN", "AMZN"),
    ("stocks2", "XYZ", "HPE"),
]

for table, old, new in updates:
    cur.execute(
        f"UPDATE {table} SET stock_symbol = %s WHERE stock_symbol = %s;",
        (new, old)
    )

# Commit changes and close
conn.commit()
cur.close()
conn.close()

print("Tables created and updated successfully.")
