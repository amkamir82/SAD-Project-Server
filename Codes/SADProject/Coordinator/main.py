from flask import Flask
from API.ClientAPI.api import api_blueprint

app = Flask(__name__)

app.register_blueprint(api_blueprint, url_prefix='/')

if __name__ == '__main__':
    app.run(port=5000, debug=True)
