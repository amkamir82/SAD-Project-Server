import json
import os.path
import threading

from coordinator_database import config

FILE_ENCODING = 'utf8'

lock = threading.Lock()


def init_clients_file():
    path = f'./{config.CLIENT_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            f.write(json.dumps({"clients": []}))


def add_client(data):
    with lock:
        init_clients_file()
        json_data = None
        with open(config.CLIENT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.CLIENT_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            json_data["clients"].append(data)
            f.write(json.dumps(json_data))
            f.close()


def get_all_clients():
    with lock:
        init_clients_file()
        with open(config.CLIENT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)

    return json_data["clients"]


def init_heartbeat_file():
    path = f'./{config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            f.write(json.dumps({"clients": {}}))


def update_heartbeat(client_url, time):
    with lock:
        init_heartbeat_file()
        json_data = None
        with open(config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            json_data["clients"][client_url] = time
            f.write(json.dumps(json_data))
            f.close()


def get_all_heartbeats():
    with lock:
        init_heartbeat_file()
        with open(config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)

    return json_data["clients"]


def delete_heartbeat(client_url):
    with lock:
        init_heartbeat_file()
        json_data = None
        with open(config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.CLIENT_HEARTBEAT_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            del json_data["clients"][client_url]
            f.write(json.dumps(json_data))
            f.close()
