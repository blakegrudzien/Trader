from flask import Blueprint, render_template, request, jsonify, current_app
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import json

main = Blueprint('main', __name__)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj) if not np.isnan(obj) else None
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return super().default(obj)


@main.route('/')
def index():
    return render_template('index.html')

@main.route('/historical-data')
def historical_data_page():
    return render_template('historical_data.html')

@main.route('/api/historical-data')
def get_historical_data():
    try:
        symbol = request.args.get('symbol', '').strip()
        start_date = request.args.get('start_date', '').strip()
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d')).strip()

        if not symbol:
            return jsonify({"error": "No symbol provided"}), 400
        if not start_date:
            return jsonify({"error": "No start date provided"}), 400

        current_app.logger.debug(f"Fetching data for {symbol} from {start_date} to {end_date}")
        
        data = fetch_stock_data(symbol, start_date, end_date)
        processed_data = process_stock_data(data)
        
        return json.dumps(processed_data, cls=CustomJSONEncoder)
    except Exception as e:
        current_app.logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

def fetch_stock_data(symbol, start_date, end_date):
    stock = yf.Ticker(symbol)
    data = stock.history(start=start_date, end=end_date)
    if data.empty:
        raise ValueError(f"No data available for {symbol} in the specified date range")
    return data

def process_stock_data(data):
    data = data.reset_index()
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    data['Daily_Return'] = data['Close'].pct_change()
    return data[['Date', 'Close', 'Daily_Return']].to_dict(orient='records')


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d')
        if pd.isna(obj):
            return None
        return super(NpEncoder, self).default(obj)

@main.route('/configure-simulation')
def configure_simulation():
    return render_template('configure_simulation.html')

def fetch_stock_data(symbol, start_date, end_date):
    stock = yf.Ticker(symbol)
    data = stock.history(start=start_date, end=end_date)
    return data

def process_stock_data(data):
    data = data.reset_index()
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    data['Daily_Return'] = data['Close'].pct_change()
    return data[['Date', 'Close', 'Daily_Return']].to_dict(orient='records')



@main.route('/run-simulation', methods=['POST'])
def run_simulation():
    data = request.json
    # Process the simulation based on the received data
    # For now, we'll just return a dummy response
    return jsonify({
        "message": "Simulation completed",
        "results": {
            "total_return": 15.5,
            "annualized_return": 7.2,
            "max_drawdown": 12.3
        }
    }), 200




