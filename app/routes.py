from flask import request, jsonify
from app.models import User, Portfolio
from flask import Blueprint, render_template
from . import db

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

# Add other routes as needed

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')
'''
@app.route('/create_user', methods=['POST'])
def create_user():
    data = request.json
    new_user = User(username=data['username'], email=data['email'])
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"message": "User created successfully"}), 201

@app.route('/create_portfolio', methods=['POST'])
def create_portfolio():
    data = request.json
    new_portfolio = Portfolio(name=data['name'], user_id=data['user_id'])
    db.session.add(new_portfolio)
    db.session.commit()
    return jsonify({"message": "Portfolio created successfully"}), 201
*/
'''