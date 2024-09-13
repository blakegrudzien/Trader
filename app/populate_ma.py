
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from app import create_app, db
from app.models import MovingAverage50, MovingAverage100
from sqlalchemy import text, inspect

def fetch_stock_data(symbol, start_date, end_date):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(start=start_date, end=end_date)
        print(f"Fetched {len(data)} rows for {symbol}")
        return data['Close']
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def calculate_moving_averages(data):
    if data is None or len(data) == 0:
        print("No data to calculate moving averages")
        return None
    ma_data = pd.DataFrame({
        'MA50': data.rolling(window=50).mean(),
        'MA100': data.rolling(window=100).mean()
    })
    print(f"Calculated MAs. First few rows: \n{ma_data.head()}")
    return ma_data

def ensure_column_exists(table, column_name):
    with db.engine.connect() as conn:
        inspector = inspect(db.engine)
        if column_name not in [c['name'] for c in inspector.get_columns(table.__tablename__)]:
            conn.execute(text(f"ALTER TABLE {table.__tablename__} ADD COLUMN {column_name} FLOAT"))
            conn.commit()
            print(f"Added column {column_name} to {table.__tablename__}")
    db.metadata.clear()
    db.reflect()

def populate_ma_tables():
    app = create_app()
    with app.app_context():
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META']
        
        for symbol in symbols:
            ensure_column_exists(MovingAverage50, symbol.lower())
            ensure_column_exists(MovingAverage100, symbol.lower())
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
        
        for symbol in symbols:
            print(f"Processing {symbol}...")
            
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if stock_data is None:
                continue
            
            ma_data = calculate_moving_averages(stock_data)
            if ma_data is None:
                continue
            
            # Verify data at multiple points
            verification_points = [0, len(ma_data)//2, -1]  # Start, middle, end
            
            for i, (date, row) in enumerate(ma_data.iterrows()):
                try:
                    # 50-day MA
                    ma50_record = MovingAverage50.query.filter_by(date=date.date()).first()
                    if not ma50_record:
                        ma50_record = MovingAverage50(date=date.date())
                        db.session.add(ma50_record)
                    setattr(ma50_record, symbol.lower(), row['MA50'])
                    
                    # 100-day MA
                    ma100_record = MovingAverage100.query.filter_by(date=date.date()).first()
                    if not ma100_record:
                        ma100_record = MovingAverage100(date=date.date())
                        db.session.add(ma100_record)
                    setattr(ma100_record, symbol.lower(), row['MA100'])
                    
                    # Verify data at specific points
                    if i in verification_points:
                        print(f"Verification for {symbol} on {date.date()}:")
                        print(f"50-day MA: {getattr(ma50_record, symbol.lower())}")
                        print(f"100-day MA: {getattr(ma100_record, symbol.lower())}")
                        print("---")
                    
                    # Commit every 100 records
                    if i % 100 == 0:
                        db.session.commit()
                        print(f"Committed batch for {symbol} (record {i})")
                
                except Exception as e:
                    print(f"Error processing {symbol} for date {date}: {e}")
                    db.session.rollback()
            
            try:
                db.session.commit()
                print(f"Final commit for {symbol}")
            except Exception as e:
                db.session.rollback()
                print(f"Error in final commit for {symbol}: {e}")
        
        print("All data has been processed.")

        # Final verification
        print("\nFinal Data Verification:")
        for symbol in symbols:
            latest_50 = MovingAverage50.query.order_by(MovingAverage50.date.desc()).first()
            latest_100 = MovingAverage100.query.order_by(MovingAverage100.date.desc()).first()
            if latest_50 and latest_100:
                print(f"{symbol}:")
                print(f"Latest 50-day MA ({latest_50.date}): {getattr(latest_50, symbol.lower())}")
                print(f"Latest 100-day MA ({latest_100.date}): {getattr(latest_100, symbol.lower())}")

        
                print("---")
def inspect_db_schema():
    inspector = inspect(db.engine)
    for table_name in inspector.get_table_names():
        print(f"\nTable: {table_name}")
        for column in inspector.get_columns(table_name):
            print(f"  {column['name']}: {column['type']}")

if __name__ == "__main__":
    populate_ma_tables()
    inspect_db_schema()