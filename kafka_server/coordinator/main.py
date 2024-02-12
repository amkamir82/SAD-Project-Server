import os
import sys

from flask import Flask

from api.client.api import api_blueprint as client_api
from api.broker.api import api_blueprint as broker_api

from coordinator.services.client import subscribe as client_subscribe_service

COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

app = Flask(__name__)

app.register_blueprint(client_api, name="client_api", url_prefix='/client')
app.register_blueprint(broker_api, name="broker_api", url_prefix='/broker')

if __name__ == '__main__':
    # client_subscribe_service.run_check_heartbeat_job()

    app.run(host='127.0.0.1', port=5000, debug=True)
