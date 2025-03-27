import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Define start time (beginning of the day)
start_time = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)  # Assume market opens at 9:00 AM

# Define number of seconds in a trading day (e.g., 9 AM to 3:30 PM = 6.5 hours = 23,400 seconds)
num_seconds = 23400  

# Generate synthetic stock market data
data = []
stock_symbol = "XYZ"

for i in range(num_seconds):
    timestamp = start_time + timedelta(seconds=i)
    price = round(random.uniform(100, 200), 2)  # Random price between 100 and 200
    volume = random.randint(100, 10000)  # Trade volume
    bid_price = round(price - random.uniform(0.1, 1), 2)  # Bid price lower than market price
    ask_price = round(price + random.uniform(0.1, 1), 2)  # Ask price higher than market price
    spread = round(ask_price - bid_price, 2)  # Spread difference

    data.append([timestamp, stock_symbol, price, volume, bid_price, ask_price, spread])

# Create DataFrame
df = pd.DataFrame(data, columns=["timestamp", "stock", "price", "volume", "bid_price", "ask_price", "spread"])

# Save to CSV
df.to_csv("stock_data.csv", index=False)

print("Generated stock price data for one full trading day and saved to 'stock_data.csv'.")
