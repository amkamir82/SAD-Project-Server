from flask import Blueprint, jsonify, request
from coordinator.services.broker import database as broker_database
import requests
import random

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init', methods=['GET'])
def init_broker():
    remote_addr = (request.headers.environ["REMOTE_ADDR"], request.headers.environ["REMOTE_PORT"])
    broker_id = request.data.decode("utf-8")
    response_code = broker_database.add_broker_to_database(broker_id,remote_addr)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    response_code, response_data = broker_database.get_broker_replica_url(broker_id)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    if len(response_data) > 1:
        keys = response_data.keys()
        key = random.choice(list(keys))
        replica_url = response_data[key]
        response_code = broker_database.add_replica_for_broker(broker_id, replica_url)
        if response_code != 200:
            return jsonify("Error during initializing broker"), response_code
        return jsonify(replica_url), 200

    replica = response_data[0]
    return jsonify("Broker successfully initialized"), 200


@api_blueprint.route('/list', methods=['GET'])
def list_all_brokers():
    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200
