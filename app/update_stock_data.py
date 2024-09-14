import sys
import os
import time
from sqlalchemy.exc import OperationalError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from app import create_app, db
from app.models import StockData
from sqlalchemy.dialects.sqlite import insert


def reset_database():
    app = create_app()
    with app.app_context():
        # Drop all tables
        db.drop_all()
        print("All tables dropped.")

        # Recreate all tables
        db.create_all()
        print("All tables recreated.")

def get_stock_symbols():
    # Replace this with actual NYSE symbols or read from a file
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'FB']

def fetch_and_process_stock_data(symbol):
    end_date = datetime.now()
    start_date = datetime(2000, 1, 1)  # Adjust this date as needed

    stock = yf.Ticker(symbol)
    data = stock.history(start=start_date, end=end_date)

    # Calculate 50-day moving average
    data['MA50'] = data['Close'].rolling(window=50).mean()
    data['MA100'] = data['Close'].rolling(window=100).mean()

    batch_size = 100  # Process 100 records at a time
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        retry_count = 0
        while retry_count < 5:  # Try up to 5 times
            try:
                for date, row in batch.iterrows():
                    stmt = insert(StockData).values(
                        symbol=symbol,
                        date=date.date(),
                        close_price=row['Close'],
                        ma_50=row['MA50'],
                        ma_100 = row['MA100'],
                        volume=row['Volume']
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['symbol', 'date'],
                        set_=dict(
                            close_price=stmt.excluded.close_price,
                            ma_50=stmt.excluded.ma_50,
                            ma_100=stmt.excluded.ma_100,
                            volume=stmt.excluded.volume
                        )
                    )
                    db.session.execute(stmt)
                db.session.commit()
                break  # If successful, break out of the retry loop
            except OperationalError as e:
                if "database is locked" in str(e):
                    retry_count += 1
                    print(f"Database locked, retrying in 5 seconds... (Attempt {retry_count})")
                    time.sleep(5)  # Wait for 5 seconds before retrying
                else:
                    raise  # If it's a different error, re-raise it
        else:
            print(f"Failed to process batch for {symbol} after 5 attempts")

    print(f"Completed processing {symbol}")

def main():
    app = create_app()
    with app.app_context():
        symbols = get_stock_symbols()
       

        for symbol in tqdm(symbols, desc="Processing stocks"):
            print(f"Fetching data for {symbol}")
            fetch_and_process_stock_data(symbol)

if __name__ == "__main__":
    main()