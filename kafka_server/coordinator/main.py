import os
import sys

from flask import Flask


COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

from api.client.api import api_blueprint as client_api
from api.broker.api import api_blueprint as broker_api

app = Flask(__name__)

app.register_blueprint(client_api, name="client_api", url_prefix='/client')
app.register_blueprint(broker_api, name="broker_api", url_prefix='/broker')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
