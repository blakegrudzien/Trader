import sys
import os
import time


import os
import sys
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy.exc import OperationalError
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from app import create_app, db
from app.models import StockData
from sqlalchemy.dialects.sqlite import insert

def get_stock_symbols():
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
              'WEC', 'WELL', 'WFC', 'WLTW', 'WM', 'WMB', 'WMT', 'WRB', 'WST', 'WTW', 'WYNN', 'XEL', 'XOM', 'XPO', 'XRAY', 'XRX', 'Xylem', 'YUM', 'ZBH', 'ZBRA', 'ZION', 'ZTS']
    pass

def fetch_latest_stock_data(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=2)  # Fetch 2 days of data to ensure we get the latest trading day

    try:
        stock = yf.Ticker(symbol)
        data = stock.history(start=start_date, end=end_date)

        if data.empty:
            print(f"No data available for {symbol}")
            return None

        # Calculate 50-day and 100-day moving averages
        data['MA50'] = stock.history(period='1mo')['Close'].rolling(window=50).mean().tail(2)
        data['MA100'] = stock.history(period='3mo')['Close'].rolling(window=100).mean().tail(2)

        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
        return None

def update_stock_data(symbol, data):
    if data is None or data.empty:
        print(f"No data to update for {symbol}")
        return

    for date, row in data.iterrows():
        # Check for NaN values and replace with None
        close_price = None if pd.isna(row['Close']) else row['Close']
        ma_50 = None if pd.isna(row['MA50']) else row['MA50']
        ma_100 = None if pd.isna(row['MA100']) else row['MA100']
        volume = None if pd.isna(row['Volume']) else int(row['Volume'])

        # Skip this record if close_price is None
        if close_price is None:
            print(f"Skipping {symbol} for date {date.date()} due to missing close price")
            continue

        stmt = insert(StockData).values(
            symbol=symbol,
            date=date.date(),
            close_price=close_price,
            ma_50=ma_50,
            ma_100=ma_100,
            volume=volume
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
        try:
            db.session.execute(stmt)
            db.session.commit()
        except IntegrityError as e:
            db.session.rollback()
            print(f"IntegrityError for {symbol} on {date.date()}: {str(e)}")
        except Exception as e:
            db.session.rollback()
            print(f"Error updating {symbol} on {date.date()}: {str(e)}")

def daily_update():
    app = create_app()
    with app.app_context():
        symbols = get_stock_symbols()
        if not symbols:
            print("No symbols to update. Check get_stock_symbols() function.")
            return
        
        for symbol in tqdm(symbols, desc="Updating stocks"):
            print(f"Fetching latest data for {symbol}")
            data = fetch_latest_stock_data(symbol)
            if data is not None:
                update_stock_data(symbol, data)
            else:
                print(f"Skipping update for {symbol} due to data fetch failure")

if __name__ == "__main__":
    daily_update()