import json
import requests


def add_broker_to_database(broker_id, remote_addr):
    data = json.dumps({"broker_id": broker_id, "remote_addr": remote_addr})
    r = requests.post(
        'http://127.0.0.1:5001/broker/add',
        data=data,
        timeout=2,
    )
    return r.status_code


def list_all_brokers():
    r = requests.get('http://127.0.0.1:5001/broker/list_all', timeout=2)
    if r.status_code != 200:
        return r.status_code, None

    response_data = json.loads(r.content.decode('utf-8'))
    return r.status_code, response_data


def get_broker_replica_url(broker_id):
    r = requests.get(
        'http://127.0.0.1:5001/broker/get_replica',
        data=broker_id,
        timeout=2,
    )
    if r.status_code != 200:
        return r.status_code, None

    response_data = json.loads(r.content.decode('utf-8'))
    return r.status_code, response_data


def list_of_replicas():
    r = requests.get(
        'http://127.0.0.1:5001/broker/list_of_replicas',
        timeout=2,
    )
    if r.status_code != 200:
        return r.status_code, None

    response_data = json.loads(r.content.decode('utf-8'))
    return r.status_code, response_data


def add_replica_for_broker(broker_id, replica):
    r = requests.post(
        'http://127.0.0.1:5001/broker/add_replica',
        data=json.dumps({"broker_id": broker_id, "replica": replica}),
        timeout=2,
    )
    return r.status_code


def update_heartbeat_status(broker_id, time):
    r = requests.post(
        "http://127.0.0.1:5001/broker/add_heartbeat",
        data=json.dumps({"broker_url": broker_id, "time": f"{time}"}),
        timeout=2,
    )
    return r.status_code
