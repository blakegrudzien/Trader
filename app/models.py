from sqlalchemy import UniqueConstraint
from . import db

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

class Portfolio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', backref=db.backref('portfolios', lazy=True))

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