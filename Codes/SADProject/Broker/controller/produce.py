from flask import Flask, request, jsonify

from Codes.SADProject.Broker.file.Indexer import Indexer
from Codes.SADProject.Broker.file.write import Write

app = Flask(__name__)


@app.route('/')
def welcome():
    return f'Welcome to the Broker API!'


@app.route('/write', methods=['POST'])
def write():
    try:
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

        print(f'key: {key}, value: {value}')

        return jsonify({'status': 'Data read successfully.'}), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # port = int(input())
    app.run(port=5003, debug=True)
