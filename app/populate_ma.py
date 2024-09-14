import os
import sys
import logging
from datetime import datetime
import sqlite3

from sqlalchemy import delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import db, create_app
from app.models import StockData, MA50, MA100
from sqlalchemy.exc import SQLAlchemyError


def get_file_mod_time(file_path):
    return datetime.fromtimestamp(os.path.getmtime(file_path))

def test_database_write(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, test_column TEXT)")
        cursor.execute("INSERT INTO test_table (test_column) VALUES (?)", (datetime.now().isoformat(),))
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    


def create_ma_tables():
    # Get unique company symbols
    companies = db.session.query(StockData.symbol).distinct().all()
    companies = [company[0] for company in companies]

    # Dynamically add columns for each company
    for company in companies:
        if not hasattr(MA50, company):
            setattr(MA50, company, db.Column(db.Numeric(10, 2)))
        if not hasattr(MA100, company):
            setattr(MA100, company, db.Column(db.Numeric(10, 2)))

    # Create the tables
    db.create_all()

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def populate_ma_tables():
    logger.info("Starting populate_ma_tables() function")
    try:
        # Get unique dates
        logger.info("Querying unique dates from StockData")
        dates = db.session.query(StockData.date).distinct().order_by(StockData.date).all()
        dates = [date[0] for date in dates]
        logger.info(f"Found {len(dates)} unique dates")

        # Get unique company symbols
        logger.info("Querying unique company symbols from StockData")
        companies = db.session.query(StockData.symbol).distinct().all()
        companies = [company[0] for company in companies]
        logger.info(f"Found {len(companies)} unique companies")

        if not dates or not companies:
            logger.error("No dates or companies found in StockData. Aborting population.")
            return

        ma50_rows_added = 0
        ma100_rows_added = 0

        for date in dates:
            logger.info(f"Processing date: {date}")
            ma50_row = db.session.get(MA50, date) or MA50(date=date)
            ma100_row = db.session.get(MA100, date) or MA100(date=date)

            for company in companies:
                logger.info(f"Processing company: {company} for date: {date}")
                
                ma50_value = db.session.query(StockData.ma_50).filter(
                    StockData.symbol == company,
                    StockData.date == date
                ).first()
                
                ma100_value = db.session.query(StockData.ma_100).filter(
                    StockData.symbol == company,
                    StockData.date == date
                ).first()

                if ma50_value:
                    logger.info(f"MA50 value found for {company} on {date}: {ma50_value[0]}")
                    setattr(ma50_row, company, ma50_value[0])
                else:
                    logger.warning(f"No MA50 value found for {company} on {date}")

                if ma100_value:
                    logger.info(f"MA100 value found for {company} on {date}: {ma100_value[0]}")
                    setattr(ma100_row, company, ma100_value[0])
                else:
                    logger.warning(f"No MA100 value found for {company} on {date}")

            logger.info(f"Adding MA50 and MA100 rows for date: {date}")
            db.session.add(ma50_row)
            db.session.add(ma100_row)
            
            ma50_rows_added += 1
            ma100_rows_added += 1

            if ma50_rows_added % 100 == 0:
                logger.info(f"Processed {ma50_rows_added} rows so far")
                db.session.flush()  # Flush every 100 rows

        logger.info("Committing changes to database")
        db.session.commit()
        logger.info(f"Changes committed. Total rows added: MA50: {ma50_rows_added}, MA100: {ma100_rows_added}")

    except Exception as e:
        logger.error(f"An error occurred in populate_ma_tables: {str(e)}")
        db.session.rollback()
        raise

    finally:
        logger.info("Finished populate_ma_tables() function")

# After the populate_ma_tables() function, add these checks:
def check_ma_tables():
    logger.info("Checking MA tables after population")
    try:
        ma50_count = db.session.query(MA50).count()
        ma100_count = db.session.query(MA100).count()
        logger.info(f"Rows in MA50 table: {ma50_count}")
        logger.info(f"Rows in MA100 table: {ma100_count}")

        if ma50_count > 0:
            sample_ma50 = db.session.query(MA50).first()
            logger.info(f"Sample MA50 row: {sample_ma50.__dict__}")
        if ma100_count > 0:
            sample_ma100 = db.session.query(MA100).first()
            logger.info(f"Sample MA100 row: {sample_ma100.__dict__}")
    except Exception as e:
        logger.error(f"Error checking MA tables: {str(e)}")

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

def setup_ma_tables():
    create_ma_tables()
    clear_ma_tables()  # Add this line to clear tables before populating
    populate_ma_tables()

if __name__ == '__main__':
    app = create_app()
    with app.app_context():
        try:
            setup_ma_tables()
            check_ma_tables()  # This function remains the same as in the previous response
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")