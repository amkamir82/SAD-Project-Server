import json
import os
import threading
from coordinator_database import config

lock = threading.Lock()


def init_brokers_file():
    path = f'./{config.BROKER_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            f.write(json.dumps({"brokers": {}}))


def add_broker(broker_id, remote_addr):
    with lock:
        init_brokers_file()
        json_data = None
        with open(config.BROKER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.BROKER_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            json_data["brokers"][broker_id] = remote_addr
            f.write(json.dumps(json_data))
            f.close()


def get_all_brokers():
    with lock:
        init_brokers_file()
        with open(config.BROKER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)

    return json_data["brokers"]


def init_brokers_replicas_file():
    path = f'./{config.BROKER_REPLICA_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            replicas = {
                "replica": {
                    "1": "http://185.226.116.193:5003"
                }
            }
            f.write(json.dumps(replicas))


def get_replica_of_a_broker(broker_id):
    init_brokers_replicas_file()
    with open(config.BROKER_REPLICA_FILE_PATH, 'r', encoding='utf8') as f:
        json_data = json.load(f)
        f.close()
        if broker_id not in json_data["replica"]:
            return json_data["replica"]
        return {broker_id: json_data["replica"][broker_id]}


def add_replica_for_a_broker(broker_id, replica):
    init_brokers_replicas_file()
    json_data = None
    with open(config.BROKER_REPLICA_FILE_PATH, 'r', encoding='utf8') as f:
        json_data = json.load(f)
        f.close()
    with open(config.BROKER_REPLICA_FILE_PATH, 'w', encoding='utf8') as f:
        json_data["replica"][broker_id] = replica
        f.write(json.dumps(json_data))
        f.close()


def init_heartbeat_file():
    path = f'./{config.BROKER_HEARTBEAT_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            f.write(json.dumps({"brokers": {}}))


def update_heartbeat(broker_url, time):
    with lock:
        init_heartbeat_file()
        json_data = None
        with open(config.BROKER_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.BROKER_HEARTBEAT_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            json_data["brokers"][broker_url] = time
            f.write(json.dumps(json_data))
            f.close()


def get_all_heartbeats():
    with lock:
        init_heartbeat_file()
        with open(config.BROKER_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)

    return json_data["brokers"]


def delete_heartbeat(client_url):
    with lock:
        init_heartbeat_file()
        json_data = None
        with open(config.BROKER_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.BROKER_HEARTBEAT_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            del json_data["brokers"][client_url]
            f.write(json.dumps(json_data))
            f.close()


def delete_broker(broker_id):
    with lock:
        init_brokers_file()
        json_data = None
        with open(config.BROKER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.BROKER_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            del json_data["brokers"][broker_id]
            f.write(json.dumps(json_data))
            f.close()


def list_of_replicas():
    init_brokers_replicas_file()
    with open(config.BROKER_REPLICA_FILE_PATH, 'r', encoding='utf8') as f:
        json_data = json.load(f)
        f.close()
        return json_data["replica"]
