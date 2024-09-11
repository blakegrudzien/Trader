import click
from flask.cli import with_appcontext
from . import db
from .models import User, StockData

@click.command('create-db')
@with_appcontext
def create_db_command():
    db.create_all()
    click.echo('Created database tables.')

@click.command('drop-db')
@with_appcontext
def drop_db_command():
    db.drop_all()
    click.echo('Dropped all database tables.')

@click.command('add-user')
@click.argument('username')
@click.argument('email')
@with_appcontext
def add_user_command(username, email):
    user = User(username=username, email=email)
    db.session.add(user)
    db.session.commit()
    click.echo(f'Added user: {username}')

@click.command('list-users')
@with_appcontext
def list_users_command():
    users = User.query.all()
    for user in users:
        click.echo(f'User: {user.username}, Email: {user.email}')

@click.command('add-stock-data')
@click.argument('symbol')
@click.argument('close_price')
@click.argument('ma_50')
@click.argument('ma_100')
@click.argument('volume')
@with_appcontext
def add_stock_data_command(symbol, close_price, ma_50, ma_100, volume):
    from datetime import date
    stock = StockData(symbol=symbol, date=date.today(), close_price=float(close_price), 
                      ma_50=float(ma_50), ma_100=float(ma_100), volume=int(volume))
    db.session.add(stock)
    db.session.commit()
    click.echo(f'Added stock data for: {symbol}')

@click.command('list-stock-data')
@click.argument('symbol')
@with_appcontext
def list_stock_data_command(symbol):
    stocks = StockData.query.filter_by(symbol=symbol).all()
    for stock in stocks:
        click.echo(f'Symbol: {stock.symbol}, Date: {stock.date}, Close: {stock.close_price}')

def init_app(app):
    app.cli.add_command(create_db_command)
    app.cli.add_command(drop_db_command)
    app.cli.add_command(add_user_command)
    app.cli.add_command(list_users_command)
    app.cli.add_command(add_stock_data_command)
    app.cli.add_command(list_stock_data_command)