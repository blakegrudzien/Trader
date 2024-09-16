import os
import sys
import logging
from datetime import datetime
import sqlite3

from sqlalchemy import delete, insert, inspect, Table, Column, Float, Date, MetaData, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import db, create_app
from app.models import StockData, MA50, MA100

def create_ma_tables():
    logger.info("Starting create_ma_tables() function")
    
    companies = db.session.query(StockData.symbol).distinct().all()
    companies = [company[0] for company in companies]
    logger.info(f"Found {len(companies)} unique companies")

    inspector = inspect(db.engine)
    
    for model in [MA50, MA100]:
        table_name = model.__tablename__
        if not inspector.has_table(table_name):
            logger.info(f"Creating table {table_name}")
            metadata = MetaData()
            Table(table_name, metadata,
                  Column('date', Date, primary_key=True),
                  *(Column(f'"{company.lower()}"', Float) for company in companies)
            )
            metadata.create_all(db.engine)
        else:
            logger.info(f"Table {table_name} already exists")

        existing_columns = set(column['name'] for column in inspector.get_columns(table_name))
        for company in companies:
            column_name = company.lower()
            if column_name not in existing_columns:
                logger.info(f"Adding column {column_name} to {table_name}")
                with db.engine.connect() as conn:
                    conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" FLOAT'))
                    conn.commit()

    db.Model.metadata.reflect(db.engine)
    logger.info("Refreshed database metadata")

def print_table_schema():
    logger.info("Printing table schema")
    inspector = inspect(db.engine)
    for table_name in ['moving_average_50', 'moving_average_100']:
        columns = inspector.get_columns(table_name)
        logger.info(f"Schema for {table_name}:")
        for column in columns:
            logger.info(f"  {column['name']}: {column['type']}")

def clear_ma_tables():
    logger.info("Clearing MA50 and MA100 tables")
    try:
        db.session.execute(delete(MA50))
        db.session.execute(delete(MA100))
        db.session.commit()
        logger.info("MA50 and MA100 tables cleared successfully")
    except Exception as e:
        logger.error(f"Error clearing MA tables: {str(e)}")
        db.session.rollback()


def check_row_count():
    logger.info("Checking row count in tables")
    for table in ['ma50', 'ma100']:
        sql = f"SELECT COUNT(*) FROM {table}"
        result = db.session.execute(text(sql)).scalar()
        logger.info(f"Number of rows in {table}: {result}")

def verify_data_in_db():
    logger.info("Verifying data in database")
    
    for table in ['ma50', 'ma100']:
        sql = f'SELECT * FROM "{table}" ORDER BY "date" DESC LIMIT 1'
        try:
            result = db.session.execute(text(sql)).fetchone()
            if result:
                logger.info(f"Latest row in {table}:")
                for column, value in result._mapping.items():
                    if value is not None:
                        logger.info(f'  "{column}": {value}')
            else:
                logger.warning(f"No data found in {table}")
        except Exception as e:
            logger.error(f"Error verifying data in {table}: {str(e)}")


def sanitize_column_name(name):
    return name.lower().replace('.', '_').replace('-', '_')

def populate_ma_tables():
    logger.info("Starting populate_ma_tables() function")
    
    # Top stocks we're interested in
    top_stocks = ['A', 'AAL', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 
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
    
    # Check which stocks have data in the source table
    check_stocks_sql = """
    SELECT DISTINCT symbol
    FROM stock_data
    WHERE symbol IN ({})
    """.format(','.join(f"'{stock}'" for stock in top_stocks))
    
    available_stocks = [row[0] for row in db.session.execute(text(check_stocks_sql)).fetchall()]
    
    logger.info(f"Stocks with available data: {available_stocks}")
    logger.info(f"Stocks without data: {set(top_stocks) - set(available_stocks)}")

    if not available_stocks:
        logger.warning("No data available for any of the specified stocks.")
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
    WHERE date >= '2000-01-01' AND symbol IN ({','.join(f"'{stock}'" for stock in available_stocks)})
    GROUP BY date
    """
    
    insert_ma100_sql = f"""
    INSERT OR REPLACE INTO moving_average_100 (date, {', '.join(sanitized_stocks)})
    SELECT date, {stock_sql_100}
    FROM stock_data
    WHERE date >= '2000-01-01' AND symbol IN ({','.join(f"'{stock}'" for stock in available_stocks)})
    GROUP BY date
    """
    
    try:
        db.session.execute(text(insert_ma50_sql))
        db.session.execute(text(insert_ma100_sql))
        db.session.commit()
        logger.info("Data inserted successfully")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error inserting data: {str(e)}")
    
    # Verify inserted data
    verify_sql = f"""
    SELECT date, {', '.join(sanitized_stocks)}
    FROM moving_average_50
    UNION ALL
    SELECT date, {', '.join(sanitized_stocks)}
    FROM moving_average_100
    ORDER BY date DESC
    LIMIT 10
    """
    try:
        inserted_data = db.session.execute(text(verify_sql)).fetchall()
        for row in inserted_data:
            logger.info(f"Inserted data: {row}")
    except Exception as e:
        logger.error(f"Error verifying data: {str(e)}")

    # Check for NULL values
    null_check_sql = f"""
    SELECT 
        COUNT(*) as total_rows,
        {', '.join(f"SUM(CASE WHEN {sanitize_column_name(stock)} IS NULL THEN 1 ELSE 0 END) as {sanitize_column_name(stock)}_null_count" for stock in available_stocks)}
    FROM moving_average_50
    """
    try:
        null_check_result = db.session.execute(text(null_check_sql)).fetchone()
        logger.info(f"NULL value check for MA50: {null_check_result}")
    except Exception as e:
        logger.error(f"Error checking NULL values: {str(e)}")

    logger.info("populate_ma_tables completed")

def insert_test_record():
    logger.info("Inserting test record using raw SQL")
    try:
        with db.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO moving_average_50 (date, aapl, googl)
                VALUES (:date, :aapl, :googl)
            """), {"date": "2000-01-01", "aapl": 150.5, "googl": 2800.75})
            conn.execute(text("""
                INSERT INTO moving_average_100 (date, aapl, googl)
                VALUES (:date, :aapl, :googl)
            """), {"date": "2000-01-01", "aapl": 148.5, "googl": 2750.75})
        logger.info("Test records inserted successfully")
    except Exception as e:
        logger.error(f"Error inserting test records: {str(e)}")

def verify_final_commit():
    logger.info("Verifying final commit...")
    try:
        last_ma50 = db.session.query(MA50).order_by(MA50.date.desc()).first()
        if last_ma50:
            logger.info(f"Last MA50 entry (date: {last_ma50.date}):")
            for key, value in last_ma50.__dict__.items():
                if key != '_sa_instance_state' and key != 'date' and value is not None:
                    logger.info(f"  {key}: {value}")
        else:
            logger.warning("No entries found in MA50 table")

        last_ma100 = db.session.query(MA100).order_by(MA100.date.desc()).first()
        if last_ma100:
            logger.info(f"Last MA100 entry (date: {last_ma100.date}):")
            for key, value in last_ma100.__dict__.items():
                if key != '_sa_instance_state' and key != 'date' and value is not None:
                    logger.info(f"  {key}: {value}")
        else:
            logger.warning("No entries found in MA100 table")
    except Exception as e:
        logger.error(f"Error verifying final commit: {str(e)}")

def check_stock_data():
    logger.info("Checking StockData table")
    total_rows = db.session.query(StockData).count()
    logger.info(f"Total rows in StockData: {total_rows}")

    non_null_ma50 = db.session.query(StockData).filter(StockData.ma_50 != None).count()
    non_null_ma100 = db.session.query(StockData).filter(StockData.ma_100 != None).count()
    logger.info(f"Rows with non-null MA50: {non_null_ma50}")
    logger.info(f"Rows with non-null MA100: {non_null_ma100}")

    sample = db.session.query(StockData).filter(
        StockData.ma_50 != None, 
        StockData.ma_100 != None
    ).first()
    if sample:
        logger.info(f"Sample StockData row: {sample.__dict__}")
    else:
        logger.warning("No rows found with both MA50 and MA100 non-null")

    date_range = db.session.query(db.func.min(StockData.date), db.func.max(StockData.date)).first()
    logger.info(f"Date range in StockData: {date_range}")

    companies = db.session.query(StockData.symbol).distinct().count()
    logger.info(f"Number of unique companies in StockData: {companies}")

def create_or_update_tables():
    # Use the same stock checking logic as in populate_ma_tables
    top_stocks = ['A', 'AAL', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 
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
    check_stocks_sql = """
    SELECT DISTINCT symbol
    FROM stock_data
    WHERE symbol IN ({})
    """.format(','.join(f"'{stock}'" for stock in top_stocks))
    
    available_stocks = [row[0] for row in db.session.execute(text(check_stocks_sql)).fetchall()]
    
    for table in ['moving_average_50', 'moving_average_100']:
        columns = [f"{sanitize_column_name(stock)} FLOAT" for stock in available_stocks]
        columns_sql = ", ".join(columns)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            date DATE PRIMARY KEY,
            {columns_sql}
        )
        """
        
        try:
            db.session.execute(text(create_table_sql))
            db.session.commit()
            logger.info(f"Table {table} created or updated successfully")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating or updating table {table}: {str(e)}")

def delete_ma_tables():
    logger.info("Deleting moving average tables")
    try:
        with db.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS moving_average_50"))
            conn.execute(text("DROP TABLE IF EXISTS moving_average_100"))
            conn.commit()
        logger.info("Moving average tables deleted successfully")
    except Exception as e:
        logger.error(f"Error deleting moving average tables: {str(e)}")


if __name__ == '__main__':
    app = create_app()
    with app.app_context():
        try:
            print(f"Database file path: {app.config['SQLALCHEMY_DATABASE_URI']}")
            delete_ma_tables()
            create_or_update_tables()
            populate_ma_tables()
            check_row_count()
            verify_data_in_db()
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}", exc_info=True)
        finally:
            db.session.remove()