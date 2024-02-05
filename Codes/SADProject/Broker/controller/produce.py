from flask import Flask, request, jsonify
from Codes.SADProject.Broker.file.write import Write

app = Flask(__name__)


@app.route('/')
def welcome():
    return 'Welcome to the Broker API!'


@app.route('/write', methods=['POST'])
def produce():
    try:
        # Assuming the request body is in JSON format with 'key' and 'value' fields
        data = request.get_json()
        key = data.get('key')
        value = data.get('value').encode('utf-8')

        if not key or not value:
            return jsonify({'error': 'Invalid request. Missing key or value.'}), 400

        write_instance = Write(3)
        write_instance.write_data(key, value)

        return jsonify({'status': 'Data written successfully.'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(port=5000, debug=True)
