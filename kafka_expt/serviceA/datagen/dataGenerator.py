import pandas as pd
import random
from datetime import datetime, timedelta

# Settings
start_time = datetime(2025, 3, 27, 9, 0, 0)
num_rows = 86400  # Rows per company
stock_symbols = ["XYZ", "ABC", "LMN"]  # List of stock symbols

for stock_symbol in stock_symbols:
    current_time = start_time
    rows_generated = 0
    data = []

    while rows_generated < num_rows:
        rows_this_second = min(random.randint(1, 30), num_rows - rows_generated)
        
        for _ in range(rows_this_second):
            price = round(random.uniform(100, 200), 2)
            volume = random.randint(100, 10000)
            bid_price = round(price - random.uniform(0.1, 1), 2)
            ask_price = round(price + random.uniform(0.1, 1), 2)
            spread = round(ask_price - bid_price, 2)
            
            data.append([current_time, stock_symbol, price, volume, bid_price, ask_price, spread])
        
        current_time += timedelta(seconds=1)
        rows_generated += rows_this_second

    df = pd.DataFrame(data, columns=["timestamp", "stock", "price", "volume", "bid_price", "ask_price", "spread"])
    csv_filename = f"{stock_symbol}_stock_data.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Generated {num_rows} rows for {stock_symbol} and saved to '{csv_filename}'.")
