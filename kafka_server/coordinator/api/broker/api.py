from flask import Blueprint, jsonify, request
import os
import sys
import json
import random
import datetime
from coordinator.services.broker import database as broker_database
from coordinator.services.broker import subscribe as broker_subscriber_service

COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init', methods=['GET'])
def init_broker():
    data = json.loads(request.data.decode('utf-8'))
    broker_addr = f'{data["ip"]}:{data["port"]}'
    broker_id = data["broker_id"]
    response_code = broker_database.add_broker_to_database(broker_id, broker_addr)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    response_code, all_brokers = broker_database.get_broker_replica_url(broker_id)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    if len(all_brokers) > 1:
        keys = all_brokers.keys()
        key = random.choice(list(keys))
        replica_url = all_brokers[key]
        response_code = broker_database.add_replica_for_broker(broker_id, replica_url)
        if response_code != 200:
            return jsonify("Error during initializing broker"), response_code
        return jsonify(replica_url), 200

    replica = all_brokers

    broker_subscriber_service.update_clients_brokers_list()
    return jsonify("Broker successfully initialized"), 200


@api_blueprint.route('/list', methods=['GET'])
def list_all_brokers():
    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200


@api_blueprint.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    broker_addr = f'{data["ip"]}:{data["port"]}'
    time = datetime.datetime.now().timestamp()

    response_code = broker_database.update_heartbeat_status(broker_addr, time)
    if response_code != 200:
        return jsonify("Error during send broker heartbeat to database"), response_code

    return jsonify("Broker heartbeat successfully updated"), 200
