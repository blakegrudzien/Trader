import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, db
from app.models import MovingAverages, MovingAverage50, MovingAverage100

def add_company_column(model, company_symbol):
    column_name = f"{company_symbol.lower()}"
    if not hasattr(model, column_name):
        setattr(model, column_name, db.Column(db.Float))

def migrate_data():
    app = create_app()
    with app.app_context():
        # Get all unique company symbols
        companies = [col.name.split('_')[0] for col in MovingAverages.__table__.columns 
                     if col.name.endswith('_ma50')]

        # Add columns for each company in new tables
        for company in companies:
            add_company_column(MovingAverage50, company)
            add_company_column(MovingAverage100, company)

        # Create new tables
        db.create_all()

        # Migrate data
        old_data = MovingAverages.query.all()
        for row in old_data:
            ma50_row = MovingAverage50(date=row.date)
            ma100_row = MovingAverage100(date=row.date)
            
            for company in companies:
                setattr(ma50_row, company, getattr(row, f"{company}_ma50", None))
                setattr(ma100_row, company, getattr(row, f"{company}_ma100", None))
            
            db.session.add(ma50_row)
            db.session.add(ma100_row)

        db.session.commit()
        print("Data migration completed successfully.")

if __name__ == "__main__":
    migrate_data()