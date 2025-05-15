import duckdb
import adbc_driver_postgresql.dbapi as adbc
from datetime import datetime, timedelta
from time import sleep
from queue import Queue
import os
import csv
import pyarrow as pa
from pyarrow import csv as pa_csv

# Set up DuckDB connection
duck = duckdb.connect()

cursor=duck.executemany('INSTALL substrait FROM community;LOAD substrait;')

def setup():
    DB_URI = "postgresql://postgres:123456789@localhost:5432/arrow_kafka"
    conn = adbc.connect(uri=DB_URI)
    
    # Create output directory if it doesn't exist
    os.makedirs("stock_results", exist_ok=True)
    
    # Create or truncate the single output file with headers
    output_file = "stock_results/all_stock_data.csv"
    with open(output_file, 'w', newline='') as f:
        f.write("timestamp,stock_symbol,id,price,volume,bid_price,ask_price,spread\n")
    
    return conn

def append_to_csv(arrow_table, topic, timestamp):
    # Single output file
    output_file = "stock_results/all_stock_data.csv"
    
    # Add timestamp and topic columns if they're not already in the data
    has_timestamp = 'timestamp' in arrow_table.column_names
    has_symbol = 'stock_symbol' in arrow_table.column_names
    
    # Convert to pandas to more easily manipulate before writing
    # This step could be optimized for very large datasets
    pandas_df = arrow_table.to_pandas()
    
    # Append to the CSV file without writing headers
    pandas_df.to_csv(output_file, mode='a', header=False, index=False)
    print(f"Results appended to {output_file}")

def queryDB(conn, queue):
    timeToBegin = '2025-03-27 09:00:00'
    time = datetime.strptime(timeToBegin, '%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    topics = ['ABC', 'XYZ', 'LMN']
    
    while True:
        for topic in topics:
            query = "SELECT * FROM stock_prices_2 WHERE timestamp = $1 AND stock_symbol = $2;"
            cursor.execute(query, (time, topic))
            
            event = cursor.fetch_arrow_table()
            
            # Register the arrow table
            duck.register("event_table", event)
            
            # Cast the price column to a numeric type in the query
            df = duck.execute("SELECT * FROM event_table WHERE CAST(price AS DOUBLE) > 150").fetch_arrow_table()
            
            # If we have results, append them to our single CSV file
            if df.num_rows > 0:
                append_to_csv(df, topic, time)
                
            print(df)  # Or further transform
            duck.unregister('event_table')
            
            time += timedelta(seconds=1)
            sleep(1)

def streamSimulator(q: Queue):
    conn = setup()
    queryDB(conn, q)

q = Queue()
streamSimulator(q)