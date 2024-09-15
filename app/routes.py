from flask import Blueprint, render_template, request, jsonify, current_app, Response
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import json
from app.trading import MA50_optimistic

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

@main.route('/historical-data')
def historical_data_page():
    return render_template('historical_data.html')

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
        
        json_data = json.dumps(processed_data, cls=CustomJSONEncoder)
        return Response(json_data, mimetype='application/json')
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
    
    # Convert NaN values to None
    data = data.where(pd.notna(data), None)
    
    result = data[['Date', 'Close', 'Daily_Return']].to_dict(orient='records')
    
    # Ensure all NaN values are converted to None
    for item in result:
        for key, value in item.items():
            if pd.isna(value):
                item[key] = None
    
    return result
@main.route('/configure-simulation')
def configure_simulation():
    return render_template('configure_simulation.html')


@main.route('/run_simulation', methods=['POST'])
def run_simulation():
    strategy = request.form.get('strategy')
    if strategy == '50_day_vs_100_day':
        symbol = request.form['symbol']
        start_date = datetime.strptime(request.form['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(request.form['end_date'], '%Y-%m-%d')
        initial_investment = float(request.form['initial_investment'])

        final_value, daily_prices = MA50_optimistic(symbol, start_date, end_date, initial_investment)
        
        total_return = ((final_value - initial_investment) / initial_investment) * 100

        return jsonify({
            'final_value': final_value,
            'total_return': total_return,
            'daily_prices': [(date.strftime('%Y-%m-%d'), price) for date, price in daily_prices]
        })
    else:
        return jsonify({'error': 'Unsupported strategy'}), 400