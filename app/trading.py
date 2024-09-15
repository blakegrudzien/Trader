import os
import sys
import logging
from datetime import datetime
import sqlite3

from sqlalchemy import delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.models import db, StockData  # Adjust import based on your module structure

# Set up the SQLAlchemy session
#engine = create_engine('sqlite:///your_database.db')  # Adjust the connection string
#Session = sessionmaker(bind=engine)
#session = Session()

def get_stock_data(symbol, start_date, end_date):
    return db.session.query(StockData).filter(
        StockData.symbol == symbol,
        StockData.date >= start_date,
        StockData.date <= end_date
    ).order_by(StockData.date).all()

def MA50_optimistic(symbol, start_date, end_date, initial_amount):
    # Fetch stock data
    data = get_stock_data(symbol, start_date, end_date)
    
    cash = initial_amount
    shares = 0
    position = None
    
    for i in range(1, len(data)):
        prev_row = data[i - 1]
        curr_row = data[i]
        
        # Buy condition
        if prev_row.ma_50 < prev_row.ma_100 and curr_row.ma_50 > curr_row.ma_100:
            # Buy all available stock
            shares_to_buy = cash // curr_row.close_price
            shares += shares_to_buy
            cash -= shares_to_buy * curr_row.close_price
            position = 'long'
        
        # Sell condition
        elif prev_row.ma_50 > prev_row.ma_100 and curr_row.ma_50 < curr_row.ma_100 and position == 'long':
            # Sell all shares
            cash += shares * curr_row.close_price
            shares = 0
            position = None
    
    # Final value of portfolio
    final_value = cash + shares * data[-1].close_price if data else initial_amount
    return final_value
