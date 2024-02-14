import os
import sys

from flask import Flask, jsonify

from api.client.api import api_blueprint as client_api
from api.broker.api import api_blueprint as broker_api

from coordinator.services.client import subscribe as client_subscribe_service
from coordinator.services.broker import subscribe as broker_subscribe_service

COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

app = Flask(__name__)

app.register_blueprint(client_api, name="client_api", url_prefix='/client')
app.register_blueprint(broker_api, name="broker_api", url_prefix='/broker')


@app.route('/', methods=['GET'])
def main_route():
    return jsonify("Welcome to SAD Project"), 200


if __name__ == '__main__':
    client_subscribe_service.run_check_heartbeat_job()
    broker_subscribe_service.run_check_heartbeat_job()

    coordinator_listening_addr = '0.0.0.0'
    app.run(coordinator_listening_addr, port=5000)
