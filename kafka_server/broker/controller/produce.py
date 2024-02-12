import os
import sys
import threading

from prometheus_client import make_wsgi_app
from flask import Flask, request, jsonify


BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))


from file.indexer import Indexer
from file.write import Write
from file.read import Read
from manager.env import *
from metrics import (
    coordinator_write_requests,
    coordinator_replicate_index_requests,
    coordinator_replicate_data_requests,
    coordinator_subscribe_requests,
)
from werkzeug.middleware.dispatcher import DispatcherMiddleware

app = Flask(__name__)
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})



@app.route('/')
def welcome():
    return 'Welcome to the Broker API!'


@app.route('/write', methods=['POST'])
def write():
    try:
        coordinator_write_requests.inc()
        # Assuming the request body is in JSON format with 'key' and 'value' fields
        data = request.get_json()
        key = data.get('key')
        value = data.get('value').encode('utf-8')
        print(key, value)

        write_instance = Write(get_primary_partition(), get_replica_url())
        wrote = write_instance.write_data(key, value)

        status = 200
        if not wrote:
            status = 400

        return jsonify({}), status

    except Exception as e:
        print(e, flush=True)
        return jsonify({'error': str(e)}), 500


@app.route('/replica/data', methods=['POST'])
def replicate_data():
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value').encode('utf-8')
        partition = data.get('partition')
        print(key, value, partition, flush=True)
        # coordinator_replicate_data_requests.inc()

        write_instance = Write(partition, None)
        replicated = write_instance.replicate_data(key, value)
        status = 200
        if not replicated:
            status = 400

        return jsonify({}), status

    except Exception as e:
        print(e, flush=True)
        return jsonify({'error': str(e)}), 500


@app.route('/replica/index', methods=['POST'])
def replicate_index():
    try:
        data = request.get_json()
        partition = data.get('partition')
        read = data.get('read')
        sync = data.get('sync')
        coordinator_replicate_index_requests.inc()

        indexer = Indexer(partition)
        indexer.update_read_sync(int(read), int(sync))
        return jsonify({'status': 'Data written successfully.'}), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


@app.route('/pull', methods=['GET'])
def pull():
    try:
        read = Read(get_primary_partition(), get_replica_url())
        key, value = read.pull_data()
        return jsonify({'key': key, 'value': value}), 200
        # coordinator_subscribe_requests.inc()

    except Exception as e:
        print(e, flush=True)
        return jsonify({'error': str(e)}), 500


@app.route('/ack', methods=['POST'])
def ack():
    try:
        read = Read(get_primary_partition(), get_replica_url())
        read.ack_message()
        return jsonify({}), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


from main import init
crun = threading.Thread(target=init)
crun.daemon = True
crun.start()

app.run('0.0.0.0', port=5003, debug=True)
# if __name__ == '__main__':
#     pass
