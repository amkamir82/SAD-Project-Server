from flask import Blueprint, request, jsonify
from SADProject.coordinator.services.client import database as client_database
from SADProject.coordinator.services.broker import database as broker_database

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init_client', methods=['GET'])
def init_client():
    remote_addr = (request.headers.environ["REMOTE_ADDR"], request.headers.environ["REMOTE_PORT"])

    response_code = client_database.add_client_to_database(remote_addr)
    if response_code != 200:
        return jsonify("Error during initializing client"), response_code

    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200


@api_blueprint.route('/list_all', methods=['GET'])
def list_all_clients():
    response_code, response_data = client_database.list_all_clients()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200


@api_blueprint.route('/subscribe', methods=['POST'])
def subscribe():
    return 'Hello from api!'
