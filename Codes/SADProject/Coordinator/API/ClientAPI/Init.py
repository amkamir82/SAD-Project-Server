from flask import Flask

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Client API!'


@app.route('/init')
def init():
    return [
        'http://127.0.0.1:1300',
        'http://127.0.0.1:1301',
    ]