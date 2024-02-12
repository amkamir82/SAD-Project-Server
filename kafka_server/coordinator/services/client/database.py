import requests
import json


def add_client_to_database(data):
    data = json.dumps(data)
    r = requests.post('http://127.0.0.1:5001/client/add', data=data)
    return r.status_code


def list_all_clients():
    r = requests.get('http://127.0.0.1:5001/client/list_all')
    if r.status_code != 200:
        return r.status_code, None

    response_data = json.loads(r.content.decode('utf-8'))
    return r.status_code, response_data


def add_subscription_plan(broker_url, client_url, subscription_id):
    r = requests.post("http://127.0.0.1:5001/subscribe/add", data=json.dumps(
        {"broker_url": broker_url, "client_url": client_url, "subscription_id": subscription_id}))
    return r.status_code


def update_heartbeat_status(client_url, time):
    r = requests.post("http://127.0.0.1:5001/client/heartbeat",
                      data=json.dumps({"client_url": client_url, "time": f"{time}"}))
    return r.status_code
