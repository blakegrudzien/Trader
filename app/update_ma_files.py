import os
import sys
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import db, create_app
from app.models import StockData

def sanitize_column_name(name):
    return name.lower().replace('.', '_').replace('-', '_')

def get_latest_date():
    sql = "SELECT MAX(date) FROM moving_average_50"
    result = db.session.execute(text(sql)).scalar()
    if result:
        return datetime.strptime(result, '%Y-%m-%d').date()
    return datetime(2000, 1, 1).date()

def get_stock_symbols():
    # Your existing list of stock symbols
    return ['^SPX','A', 'AAL', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 
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
              'WEC', 'WELL', 'WFC', 'WLTW', 'WM', 'WMB', 'WMT', 'WRB', 'WST', 'WTW', 'WYNN', 'XEL', 'XOM', 'XPO', 'XRAY', 'XRX', 'Xylem', 'YUM', 'ZBH', 'ZBRA', 'ZION', 'ZTS']  # Add all your symbols here

def update_ma_tables(end_date=None):
    logger.info("Starting daily update of MA tables")
    
    latest_date = get_latest_date()
    if end_date is None:
        end_date = datetime.now().date()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    if latest_date >= end_date:
        logger.info(f"Data is already up to date as of {end_date}")
        return

    start_date = latest_date + timedelta(days=1)
    
    logger.info(f"Updating data from {start_date} to {end_date}")
    
    top_stocks = get_stock_symbols()
    
    # Check which stocks have new data
    check_stocks_sql = """
    SELECT DISTINCT symbol
    FROM stock_data
    WHERE symbol IN ({}) AND date > :start_date AND date <= :end_date
    """.format(','.join(f"'{stock}'" for stock in top_stocks))
    
    available_stocks = [row[0] for row in db.session.execute(text(check_stocks_sql), 
                                                             {"start_date": start_date, "end_date": end_date}).fetchall()]
    
    logger.info(f"Stocks with new data: {available_stocks}")

    if not available_stocks:
        logger.warning("No new data available for any of the specified stocks.")
        return

    sanitized_stocks = [sanitize_column_name(stock) for stock in available_stocks]
    
    # Create the SQL for each available stock
    stock_sql_50 = ", ".join([f"""
        MAX(CASE WHEN symbol = '{stock}' THEN ma_50 END) as {sanitize_column_name(stock)}
    """ for stock in available_stocks])
    
    stock_sql_100 = ", ".join([f"""
        MAX(CASE WHEN symbol = '{stock}' THEN ma_100 END) as {sanitize_column_name(stock)}
    """ for stock in available_stocks])

    insert_ma50_sql = f"""
    INSERT OR REPLACE INTO moving_average_50 (date, {', '.join(sanitized_stocks)})
    SELECT date, {stock_sql_50}
    FROM stock_data
    WHERE date > :start_date AND date <= :end_date AND symbol IN ({','.join(f"'{stock}'" for stock in available_stocks)})
    GROUP BY date
    """
    
    insert_ma100_sql = f"""
    INSERT OR REPLACE INTO moving_average_100 (date, {', '.join(sanitized_stocks)})
    SELECT date, {stock_sql_100}
    FROM stock_data
    WHERE date > :start_date AND date <= :end_date AND symbol IN ({','.join(f"'{stock}'" for stock in available_stocks)})
    GROUP BY date
    """
    
    try:
        db.session.execute(text(insert_ma50_sql), {"start_date": start_date, "end_date": end_date})
        db.session.execute(text(insert_ma100_sql), {"start_date": start_date, "end_date": end_date})
        db.session.commit()
        logger.info("New data inserted successfully")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error inserting new data: {str(e)}")

def main(end_date=None):
    app = create_app()
    with app.app_context():
        try:
            update_ma_tables(end_date)
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}", exc_info=True)
        finally:
            db.session.remove()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])  # Use the date provided as a command-line argument
    else:
        main()  # Use the current date