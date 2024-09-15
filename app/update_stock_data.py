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
    return ['A', 'AAL', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 
            'AES', 'AFL', 'AIG', 'AIV', 'AIZ', 'AJG', 'AMAT', 'AMD', 'AME', 'AMGN', 'AMP', 'AMT', 'AMZN', 'AON', 'AOS', 
            'APA', 'APD', 'APH', 'APL', 'APTV', 'ARDX', 'ARE', 'ARNC', 'ATO', 'ATVI', 'AVB', 'AVGO', 'AVY', 'AWK', 'AWR', 
            'AXP', 'BA', 'BAC', 'BAX', 'BBWI', 'BBY', 'BDX', 'BE', 'BEN', 'BG', 'BHI', 'BIDU', 'BIG', 'BIIB', 
            'BIO', 'BK', 'BKNG', 'BKR', 'BLK', 'BLL', 'BMY', 'BND', 'BNS', 'BOH', 'BOM', 'BURL', 'BVN', 'BWXT', 'BXP', 
            'C', 'CAG', 'CAH', 'CARR', 'CAT', 'CB', 'CBOE', 'CBRE', 'CBSH', 'CBU', 'CCL', 'CDNS', 'CDW', 'CE', 'CELG', 
            'CERN', 'CF', 'CFG', 'CHD', 'CHRW', 'CHTR', 'CI', 'CINF', 'CL', 'CLX', 'CM', 'CMA', 'CMCSA', 'CME', 'CMG', 'CMI', 
            'CMS', 'CNC', 'CNP', 'COF', 'COG', 'COL', 'COO', 'COP', 'COST', 'COUP', 'COV', 'CPB', 'CPE', 'CPF', 'CPG', 'CPK', 
            'CPL', 'CPT', 'CRL', 'CRM', 'CRN', 'CRSP', 'CSCO', 'CSX', 'CTAS', 'CTSH', 'CTXS', 'CVS', 'CVX', 'CWH', 'D', 'DAL', 
            'DAN', 'DASH', 'DASHP', 'DCO', 'DHI', 'DHR', 'DHR', 'DIS', 'DISH', 'DLB', 'DLPH', 'DLPH', 'DRE', 'DRI', 'DTE', 
            'DTE', 'DTEK', 'DTV', 'DU', 'DUK', 'DVA', 'DVN', 'DXC', 'DXCM', 'EA', 'EBAY', 'ECL', 'ED', 'EFX', 'EGP', 'EIX', 
            'EL', 'EMN', 'EMR', 'ENB', 'EOG', 'EPAM', 'EQIX', 'EQR', 'ES', 'ESS', 'ETN', 'ETR', 'EVBG', 'EVR', 'EW', 'EXC', 
            'EXPE', 'EXR', 'F', 'FAST', 'FB', 'FBC', 'FBD', 'FBHS', 'FC', 'FCX', 'FDX', 'FE', 'FFIV', 'FIS', 'FISV', 'FITB', 
            'FLIR', 'FLS', 'FLWS', 'FMC', 'FMX', 'FOXA', 'FOX', 'FRT', 'FTI', 'FTNT', 'FTV', 'FUBO', 'G', 'GAS', 'GCI', 'GD', 
            'GE', 'GILD', 'GIS', 'GL', 'GLW', 'GM', 'GME', 'GNRC', 'GOOG', 'GOOGL', 'GPC', 'GPN', 'GPS', 'GRMN', 'GS', 'GWW', 
            'HAL', 'HAS', 'HCA', 'HCBK', 'HCI', 'HCP', 'HD', 'HES', 'HFC', 'HIG', 'HII', 'HOG', 'HOLX', 'HON', 'HPE', 'HPQ', 
            'HRL', 'HRS', 'HSIC', 'HST', 'HSY', 'HTA', 'HTZ', 'HUM', 'HUN', 'HVB', 'IBM', 'ICE', 'IDXX', 'IEX', 'IFF', 'ILMN', 
            'INCY', 'IND', 'INT', 'INTC', 'INTU', 'INVH', 'IP', 'IPG', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'JBHT', 'JCI', 
            'JCOM', 'JCP', 'JEC', 'JEF', 'JELD', 'JNJ', 'JPM', 'JWN', 'K', 'KEY', 'KIM', 'KLAC', 'KMB', 'KMI', 'KMX', 'KO', 'KORS',
              'KR', 'KSS', 'KSU', 'L', 'LB', 'LDOS', 'LEG', 'LEN', 'LH', 'LKQ', 'LLY', 'LM', 'LMT', 'LNC', 'LNCR', 'LOW', 'LRCX', 
              'LUV', 'LVS', 'LW', 'LYB', 'M', 'MA', 'MAR', 'MAS', 'MAT', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDLZ', 'MDT', 'MET', 'MGM', 
              'MHK', 'MKTX', 'MLM', 'MMC', 'MMM', 'MNST', 'MO', 'MPW', 'MRK', 'MRO', 'MS', 'MSCI', 'MSFT', 'MTB', 'MTD', 'MU', 'MUR', 
              'N', 'NAVI', 'NBL', 'NDAQ', 'NEE', 'NEM', 'NFLX', 'NFX', 'NI', 'NKE', 'NLOK', 'NLY', 'NKE', 'NNN', 'NOAH', 'NOC', 'NOV', 
              'NOW', 'NTRS', 'NUE', 'NVDA', 'NVR', 'NWL', 'NWSA', 'NWS', 'O', 'ODFL', 'OMC', 'OMI', 'ONB', 'ORCL', 'OXY', 'PAYX', 
              'PBCT', 'PCG', 'PFG', 'PGR', 'PG', 'PFE', 'PG', 'PFG', 'PH', 'PKG', 'PLD', 'PM', 'PNC', 'PNR', 'PNW', 'PPG', 'PPL', 
              'PRGO', 'PRU', 'PSA', 'PSX', 'PWR', 'PXD', 'QCOM', 'QRVO', 'R', 'RCL', 'RE', 'REG', 'REGN', 'RF', 'RHI', 'RJF', 'RL', 
              'RMD', 'ROK', 'ROL', 'ROST', 'RSG', 'RTN', 'S', 'SBAC', 'SBUX', 'SCG', 'SCHW', 'SE', 'SEE', 'SHW', 'SIVB', 'SJM', 'SLB', 
              'SLG', 'SNA', 'SNPS', 'SO', 'SPG', 'SPGI', 'SPLK', 'SPR', 'SQ', 'SRE', 'STE', 'STT', 'STX', 'STZ', 'SWK', 'SWKS', 'SYK', 
              'SYY', 'T', 'TAP', 'TDG', 'TDY', 'TE', 'TEL', 'TER', 'TFC', 'TFX', 'TGT', 'THC', 'TIF', 'TJX', 'TMO', 'TMUS', 'TOL', 'TPR', 
              'TRIP', 'TROW', 'TRV', 'TSCO', 'TSLA', 'TSN', 'TT', 'TTWO', 'TWTR', 'TXN', 'UAL', 'UDR', 'UHS', 'ULTA', 'UNH', 'UNP', 'UPS', 
              'URI', 'USAA', 'USB', 'UTX', 'V', 'VFC', 'VIAC', 'VLO', 'VMC', 'VNO', 'VRSK', 'VRSN', 'VRTX', 'VZ', 'WAB', 'WAT', 'WBA', 'WDC', 
              'WEC', 'WELL', 'WFC', 'WLTW', 'WM', 'WMB', 'WMT', 'WRB', 'WST', 'WTW', 'WYNN', 'XEL', 'XOM', 'XPO', 'XRAY', 'XRX', 'Xylem', 'YUM', 'ZBH', 'ZBRA', 'ZION', 'ZTS'
]

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