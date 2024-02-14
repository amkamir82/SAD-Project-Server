import datetime
import json
import os
import random
import sys

from coordinator.services.broker import subscribe as broker_subscribe_service
from coordinator.services.client import database as client_database
from coordinator.services.broker import database as broker_database
from flask import Blueprint, request, jsonify

COORDINATOR_PROJECT_PATH = os.getenv("COORDINATOR_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(COORDINATOR_PROJECT_PATH))

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init', methods=['POST'])
def init_client():
    data = json.loads(request.data.decode('utf-8'))
    client_addr = f'http://{data["ip"]}:{data["port"]}'

    response_code = client_database.add_client_to_database(client_addr)
    if response_code != 200:
        return jsonify("Error during initializing client"), response_code

    response_code, response_data = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    return response_data, 200


@api_blueprint.route('/list', methods=['GET'])
def list_all_clients():
    response_code, response_data = client_database.list_all_clients()
    if response_code != 200:
        return jsonify("Error during getting list of clients from database"), response_code

    return response_data, 200


@api_blueprint.route('/subscribe', methods=['POST'])
def subscribe():
    data = json.loads(request.data.decode('utf-8'))
    client_addr = f'http://{data["ip"]}:{data["port"]}'

    random_id = random.randint(1, 1000000)

    response_code, all_subscriptions = broker_subscribe_service.get_all_subscriptions()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        return jsonify("Error during getting list of brokers from database"), response_code

    min_length = 1000000
    selected_broker_id = None
    print("#######find broker for subscriber")
    for key in all_brokers.keys():
        print(key)
        print(all_brokers)
        print(all_subscriptions)
        if f"{key}:{all_brokers[key]}" not in all_subscriptions.keys():
            print("check by first if")
            selected_broker_id = key
            break
        if len(all_subscriptions[f"{key}:{all_brokers[key]}"]) < min_length:
            print("check by second if")
            min_length = len(all_subscriptions[f"{key}:{all_brokers[key]}"])
            selected_broker_id = key

    broker_data = f"{selected_broker_id}:{all_brokers[selected_broker_id]}"
    broker_url = all_brokers[selected_broker_id]

    tmp_dict = {}
    if broker_data in all_subscriptions:
        for subs in all_subscriptions[broker_data]:
            tmp_dict[subs[1]] = subs[0]
    else:
        tmp_dict[random_id] = client_addr
    response_code = broker_subscribe_service.send_subscribe_to_broker(broker_url, tmp_dict)
    if response_code != 200:
        return jsonify("Error during sending subscription to broker"), response_code

    response_code = client_database.add_subscription_plan(broker_data, client_addr, random_id)
    if response_code != 200:
        return jsonify("Error during adding subscription to database"), response_code

    return jsonify({"broker_url": broker_data, "id": random_id}), 200


@api_blueprint.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    client_addr = f'http://{data["ip"]}:{data["port"]}'
    time = datetime.datetime.now().timestamp()

    response_code = client_database.update_heartbeat_status(client_addr, time)
    if response_code != 200:
        return jsonify("Error during send client heartbeat to database"), response_code

    return jsonify("Client heartbeat successfully updated"), 200
