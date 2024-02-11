from flask import Blueprint, jsonify, request
import os
import sys
COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

from services.broker import database as broker_database
import requests

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init', methods=['GET'])
def init_broker():
    remote_addr = (request.headers.environ["REMOTE_ADDR"], request.headers.environ["REMOTE_PORT"])

    response_code = broker_database.add_broker_to_database(remote_addr)
    if response_code != 200:
        return jsonify("Error during initializing client"), response_code

    return jsonify("Broker successfully initialized"), 200


@api_blueprint.route('/list', methods=['GET'])
def list_all_brokers():
    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200
