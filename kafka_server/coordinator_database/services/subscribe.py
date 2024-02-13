import json
import threading
import os.path

from coordinator_database import config

lock = threading.Lock()


def init_subscriptions_file():
    path = f'./{config.SUBSCRIBER_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w', encoding='utf8') as f:
            f.write(json.dumps({"subscriptions": {}}))


def add_subscription(broker_url, client_url, subscription_id):
    with lock:
        init_subscriptions_file()
        json_data = None
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            if broker_url not in json_data['subscriptions']:
                json_data['subscriptions'][broker_url] = []
            json_data["subscriptions"][broker_url].append((client_url, subscription_id))
            f.write(json.dumps(json_data))
            f.close()


def delete_subscription(client_url):
    with lock:
        init_subscriptions_file()
        json_data = None
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)
            f.close()
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'w', encoding='utf8') as f:
            tmp = {"subscriptions": {}}
            for broker_url in json_data['subscriptions'].keys():
                tmp["subscriptions"][broker_url] = []
                for _, item in enumerate(json_data['subscriptions'][broker_url]):
                    if client_url not in item:
                        tmp["subscriptions"][broker_url].append(item)
            f.write(json.dumps(tmp))
            f.close()


def get_all_subscriptions():
    with lock:
        init_subscriptions_file()
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'r', encoding='utf8') as f:
            json_data = json.load(f)

    return json_data["subscriptions"]
