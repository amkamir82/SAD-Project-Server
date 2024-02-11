import requests
import json


def add_broker_to_database(data):
    data = json.dumps(data)
    r = requests.post('http://127.0.0.1:5001/broker/add', data=data)
    return r.status_code


def list_all_brokers():
    r = requests.get('http://127.0.0.1:5001/broker/list_all')
    if r.status_code != 200:
        return r.status_code, None

    response_data = json.loads(r.content.decode('utf-8'))
    return r.status_code, response_data
