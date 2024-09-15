import os
import sys
import logging
from datetime import datetime
import sqlite3
from decimal import Decimal

from sqlalchemy import delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.models import StockData
from app import db, create_app

def get_stock_data(symbol, start_date, end_date):
    return db.session.query(StockData).filter(
        StockData.symbol == symbol,
        StockData.date >= start_date,
        StockData.date <= end_date
    ).order_by(StockData.date).all()

def MA50_optimistic(symbol, start_date, end_date, initial_amount):
    # Fetch stock data
    data = get_stock_data(symbol, start_date, end_date)
    
    cash = Decimal(str(initial_amount))
    shares = Decimal('0')
    position = None
    daily_prices = []
    
    for row in data:
        daily_prices.append((row.date, Decimal(str(row.close_price))))
        
        if len(daily_prices) < 2:
            continue
        
        prev_row = data[daily_prices.index((row.date, Decimal(str(row.close_price)))) - 1]
        curr_row = row
        
        # Buy condition
        if prev_row.ma_50 < prev_row.ma_100 and curr_row.ma_50 > curr_row.ma_100:
            # Buy all available stock
            shares_to_buy = cash // Decimal(str(curr_row.close_price))
            shares += shares_to_buy
            cash -= shares_to_buy * Decimal(str(curr_row.close_price))
            position = 'long'
        
        # Sell condition
        elif prev_row.ma_50 > prev_row.ma_100 and curr_row.ma_50 < curr_row.ma_100 and position == 'long':
            # Sell all shares
            cash += shares * Decimal(str(curr_row.close_price))
            shares = Decimal('0')
            position = None
    
    # Final value of portfolio
    final_value = cash + shares * Decimal(str(data[-1].close_price)) if data else Decimal(str(initial_amount))
    return float(final_value), [(date, float(price)) for date, price in daily_prices]

def main():
    app = create_app()
    with app.app_context():
        # Example usage
        symbol = 'AAPL'
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2021, 12, 31)
        initial_amount = 10000

        result, daily_prices = MA50_optimistic(symbol, start_date, end_date, initial_amount)
        print(f"Final portfolio value: ${result:.2f}")
        print("\nDaily prices for AAPL:")
        for date, price in daily_prices:
            print(f"{date.strftime('%Y-%m-%d')}: ${price:.2f}")

if __name__ == "__main__":
    main()