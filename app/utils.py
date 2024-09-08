from app import db
from app.models import User, Portfolio

def get_user_portfolios(user_id):
    return Portfolio.query.filter_by(user_id=user_id).all()

def update_portfolio_value(portfolio_id, new_value):
    portfolio = Portfolio.query.get(portfolio_id)
    if portfolio:
        portfolio.value = new_value
        db.session.commit()
        return True
    return False