import pandas as pd
import random
from datetime import datetime, timedelta

# Define start time
start_time = datetime(2025, 3, 27, 9, 0, 0)

# Initialize variables
num_rows = 86400  # Total rows required
data = []
stock_symbol = "XYZ"
current_time = start_time
rows_generated = 0

# Generate stock market data
while rows_generated < num_rows:
    # Determine how many rows to generate for this second (1 to 30)
    rows_this_second = min(random.randint(1, 30), num_rows - rows_generated)
    
    for _ in range(rows_this_second):
        price = round(random.uniform(100, 200), 2)  # Random price between 100 and 200
        volume = random.randint(100, 10000)  # Trade volume
        bid_price = round(price - random.uniform(0.1, 1), 2)  # Bid price lower than market price
        ask_price = round(price + random.uniform(0.1, 1), 2)  # Ask price higher than market price
        spread = round(ask_price - bid_price, 2)  # Spread difference
        
        data.append([current_time, stock_symbol, price, volume, bid_price, ask_price, spread])
    
    # Move to next second
    current_time += timedelta(seconds=1)
    rows_generated += rows_this_second

# Create DataFrame
df = pd.DataFrame(data, columns=["timestamp", "stock", "price", "volume", "bid_price", "ask_price", "spread"])

# Save to CSV
df.to_csv("stock_data.csv", index=False)
print("Generated 86400 rows of stock price data and saved to 'stock_data.csv'.")