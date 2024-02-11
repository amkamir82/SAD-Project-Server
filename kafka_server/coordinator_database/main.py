from flask import Flask, request, jsonify
from coordinator_database.services import client
from coordinator_database.services import broker
import threading
import concurrent.futures

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
    data = request.data.decode('utf-8')
    thread = threading.Thread(target=broker.add_broker, args=(data,))
    thread.start()
    return jsonify({"message": "Broker successfully added"}), 200


@app.route('/broker/list_all', methods=['GET'])
def list_brokers():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(broker.get_all_broker)
        result = future.result()
    return jsonify(result)


if __name__ == '__main__':
    app.run(port=5001, debug=True)
