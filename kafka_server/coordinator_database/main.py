from flask import Flask, request, jsonify
from coordinator_database.services import client
from coordinator_database.services import broker
import threading
import concurrent.futures
import json

app = Flask(__name__)


@app.route('/client/add', methods=['POST'])
def add_client():
    data = request.data.decode('utf-8')
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
    print()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.add_replica_for_a_broker, broker_id, replica)
        result = future.result()
    return jsonify("Successfully added replica"), 200


if __name__ == '__main__':
    app.run(port=5001, debug=True)
