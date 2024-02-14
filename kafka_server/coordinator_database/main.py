import concurrent.futures
from logging import Logger
import json
import threading

from flask import Flask, request, jsonify
from coordinator_database.services import client, broker, subscribe

app = Flask(__name__)
logger = Logger(__name__)


@app.route('/client/add', methods=['POST'])
def add_client():
    data = json.loads(request.data.decode('utf-8'))
    thread = threading.Thread(target=client.add_client, args=(data,))
    thread.start()
    return jsonify({"message": "Client successfully added"}), 200


@app.route('/client/list_all', methods=['GET'])
def list_clients():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(client.get_all_clients)
        result = future.result()
    return jsonify(result)


@app.route('/broker/add', methods=['POST'])
def add_broker():
    data = json.loads(request.data.decode('utf-8'))
    broker_id = data['broker_id']
    remote_addr = data['remote_addr']
    thread = threading.Thread(target=broker.add_broker, args=(broker_id, remote_addr,))
    thread.start()
    return jsonify({"message": "Broker successfully added"}), 200


@app.route('/broker/list_all', methods=['GET'])
def list_brokers():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.get_all_brokers)
        result = future.result()
    return jsonify(result)


@app.route('/broker/delete', methods=['POST'])
def delete_broker():
    data = json.loads(request.data.decode('utf-8'))
    broker_id = data['broker_id']
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.delete_broker, broker_id)
        result = future.result()
    return jsonify("Successfully deleted")


@app.route('/broker/get_replica', methods=['GET'])
def get_replica_of_a_broker():
    broker_id = request.data.decode('utf-8')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.get_replica_of_a_broker, broker_id)
        result = future.result()
    return jsonify(result)


@app.route('/broker/add_replica', methods=['POST'])
def add_replica_for_a_broker():
    data = json.loads(request.data.decode('utf-8'))
    broker_id = data["broker_id"]
    replica = data["replica"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.add_replica_for_a_broker, broker_id, replica)
        _ = future.result()
    return jsonify("Successfully added replica"), 200


@app.route('/subscribe/add', methods=['POST'])
def add_subscription_plan():
    data = json.loads(request.data.decode('utf-8'))
    broker_url = data["broker_url"]
    client_url = data["client_url"]
    subscription_id = data["subscription_id"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(
            subscribe.add_subscription,
            broker_url,
            client_url,
            subscription_id,
        )
        _ = future.result()
    return jsonify("Successfully added replica"), 200


@app.route('/client/heartbeat', methods=['POST'])
def update_client_heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    client_url = data["client_url"]
    time = data["time"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(client.update_heartbeat, client_url, time)
        _ = future.result()
    return jsonify("Successfully added replica"), 200


@app.route('/client/list_all_heartbeats', methods=['GET'])
def list_all_client_heartbeats():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(client.get_all_heartbeats)
        result = future.result()
    return jsonify(result), 200


@app.route('/client/delete_heartbeat', methods=['POST'])
def delete_client_heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    client_url = data["client_url"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            future = executor.submit(client.delete_heartbeat, client_url)
            _ = future.result()
        except Exception as e:
            logger.error(e)
            return jsonify("Error deleting subscription"), 500
    return jsonify("Successfully deleting client heartbeat"), 200


@app.route('/subscribe/delete', methods=['POST'])
def unsubscribe():
    data = json.loads(request.data.decode('utf-8'))
    client_url = data["client_url"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            future = executor.submit(subscribe.delete_subscription, client_url)
            _ = future.result()
        except Exception as e:
            logger.error(e)
            return jsonify("Error deleting subscription"), 500
    return jsonify("Successfully deleted "), 200


@app.route('/subscribe/list_all', methods=['GET'])
def get_all_subscriptions():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            future = executor.submit(subscribe.get_all_subscriptions)
            result = future.result()
        except Exception as e:
            logger.error(e)
            return jsonify("Error deleting subscription"), 500
    return jsonify(result), 200


@app.route('/broker/add_heartbeat', methods=['POST'])
def update_broker_heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    broker_url = data["broker_url"]
    time = data["time"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.update_heartbeat, broker_url, time)
        _ = future.result()
    return jsonify("Successfully added replica"), 200


@app.route('/broker/list_all_heartbeats', methods=['GET'])
def list_all_broker_heartbeats():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.get_all_heartbeats)
        result = future.result()
    return jsonify(result), 200


@app.route('/broker/delete_heartbeat', methods=['POST'])
def delete_broker_heartbeat():
    data = json.loads(request.data.decode('utf-8'))
    broker_url = data["broker_url"]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            future = executor.submit(broker.delete_heartbeat, broker_url)
            _ = future.result()
        except Exception as e:
            logger.error(e)
            return jsonify("Error deleting broker heartbeat"), 500
    return jsonify("Successfully deleting broker heartbeat"), 200


@app.route('/broker/list_of_replicas', methods=['GET'])
def list_of_replicas():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            future = executor.submit(broker.list_of_replicas)
            result = future.result()
        except Exception as e:
            logger.error(e)
            return jsonify("Error deleting broker heartbeat"), 500
    return jsonify(result), 200


if __name__ == '__main__':
    app.run(port=5001)
