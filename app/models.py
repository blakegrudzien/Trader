from sqlalchemy import UniqueConstraint
from . import db
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import column_property


class DynamicBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def __declare_last__(cls):
        for name, column in cls.additional_columns.items():
            setattr(cls, name, column_property(column))

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

class Portfolio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', backref=db.backref('portfolios', lazy=True))

class Daily_price(DynamicBase, db.Model):
    date = db.Column(db.Date, primary_key=True)
    additional_columns = {}

class MA50(DynamicBase, db.Model):
    date = db.Column(db.Date, primary_key=True)
    additional_columns = {}

class MA100(DynamicBase, db.Model):
    date = db.Column(db.Date, primary_key=True)
    additional_columns = {}

def add_company_column(model, company_symbol):
    column_name = company_symbol.lower()
    if column_name not in model.additional_columns:
        model.additional_columns[column_name] = db.Column(db.Float)

class StockData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(10), nullable=False)
    date = db.Column(db.Date, nullable=False)
    close_price = db.Column(db.Numeric(10, 2), nullable=False)
    ma_50 = db.Column(db.Numeric(10, 2))
    ma_100 = db.Column(db.Numeric(10, 2))
    volume = db.Column(db.BigInteger)

    __table_args__ = (
        UniqueConstraint('symbol', 'date', name='uix_symbol_date'),
    )

    def __repr__(self):
        return f"<StockData {self.symbol} {self.date}>"
    

class TradingResult(db.Model):
    __tablename__ = 'trading_results'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(10), nullable=False)
    date = db.Column(db.Date, nullable=False)
    cumulative_return = db.Column(db.Numeric(10, 2), nullable=False)
    
    def __repr__(self):
        return f"<TradingResult {self.symbol} {self.date} {self.cumulative_return}>"