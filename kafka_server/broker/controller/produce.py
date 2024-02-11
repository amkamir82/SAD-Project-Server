import time
import os
import sys

from prometheus_client import make_wsgi_app
from flask import Flask, request, jsonify

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))


from file.indexer import Indexer
from file.write import Write
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

        if not key or not value:
            return jsonify({'error': 'Invalid request. Missing key or value.'}), 400

        write_instance = Write('3', 'http://localhost:5001')
        write_instance.write_data(key, value)

        return jsonify({'status': 'Data written successfully.'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/replica/data', methods=['POST'])
def replicate_data():
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value').encode('utf-8')
        coordinator_replicate_data_requests.inc()

        if not key or not value:
            return jsonify({'error': 'Invalid request. Missing key or value.'}), 400

        write_instance = Write('3', 'localhost:5000/')
        replicated = write_instance.replicate_data(key, value)
        status = 200
        if not replicated:
            status = 400

        return jsonify({'status': 'Data written successfully.'}), status

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


@app.route('/replica/index', methods=['POST'])
def replicate_index():
    try:
        data = request.get_json()
        partition = data.get('partition')
        read = data.get('read')
        sync = data.get('sync')
        coordinator_replicate_index_requests.inc()

        indexer = Indexer(partition, '')
        indexer.update_read_sync(int(read), int(sync))
        return jsonify({'status': 'Data written successfully.'}), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


@app.route('/subscribe', methods=['POST'])
def replicate():
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value')
        coordinator_subscribe_requests.inc()

        print(f'key: {key}, value: {value}')

        return jsonify({'status': 'Data read successfully.'}), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # port = int(input())
    app.run('0.0.0.0', port=5003, debug=True)
