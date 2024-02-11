import threading
from coordinator_database import config
import json
import os.path

lock = threading.Lock()


def init_subscriptions_file():
    path = f'./{config.SUBSCRIBER_DATABASE_FILE_PATH}'
    check_file = os.path.isfile(path)
    if not check_file:
        with open(path, 'w') as f:
            f.write(json.dumps({"subscriptions": {}}))


def add_subscription(broker_url, client_url, subscription_id):
    with lock:
        init_subscriptions_file()
        json_data = None
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'r') as f:
            json_data = json.load(f)
            f.close()
        with open(config.SUBSCRIBER_DATABASE_FILE_PATH, 'w') as f:
            if client_url not in json_data['subscriptions']:
                json_data['subscriptions'][client_url] = []
            json_data["subscriptions"][client_url].append((broker_url, subscription_id))
            f.write(json.dumps(json_data))
            f.close()
