from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Client api!'


@app.route('/init')
def init():
    mock = [
        'http://127.0.0.1:1300',
        'http://127.0.0.1:1301',
    ]
    return jsonify(mock)