import datetime
import json
import os
import sys

COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))
from services.broker import subscribe as broker_subscriber_service
from services.broker import database as broker_database
import config
from flask import Blueprint, jsonify, request


api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init', methods=['GET'])
def init_broker():
    data = json.loads(request.data.decode('utf-8'))
    broker_addr = f'http://{data["ip"]}:{data["port"]}'
    broker_id = data["broker_id"]
    response_code = broker_database.add_broker_to_database(broker_id, broker_addr)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting all brokers"), response_code

    response_code, all_brokers_replicas = broker_database.get_broker_replica_url(broker_id)
    if response_code != 200:
        return jsonify("Error during initializing broker"), response_code

    # if len(all_brokers) > 1:
    #     keys = all_brokers.keys()
    #     key = random.choice(list(keys))
    #     replica_url = all_brokers[key]
    #     response_code = broker_database.add_replica_for_broker(broker_id, replica_url)
    #     if response_code != 200:
    #         return jsonify("Error during initializing broker"), response_code
    #     return jsonify(replica_url), 200

    replica_url = all_brokers_replicas[broker_id]
    partition_count = len(all_brokers)
    master_coordinator_url = config.MASTER_COORDINATOR_URL
    backup_coordinator_url = config.BACKUP_COORDINATOR_URL
    broker_subscriber_service.update_brokers_subscriptions()
    return jsonify({"replica_url": replica_url, "partition_count": partition_count,
                    "master_coordinator_url": master_coordinator_url,
                    "replica_coordinator_url": backup_coordinator_url}), 200


@api_blueprint.route('/list', methods=['GET'])
def list_all_brokers():
    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200


@api_blueprint.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    broker_addr = f'http://{data["ip"]}:{data["port"]}'
    time = datetime.datetime.now().timestamp()

    response_code = broker_database.update_heartbeat_status(broker_addr, time)
    if response_code != 200:
        return jsonify("Error during send broker heartbeat to database"), response_code

    return jsonify("Broker heartbeat successfully updated"), 200
