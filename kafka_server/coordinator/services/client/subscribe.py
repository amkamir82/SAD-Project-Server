import threading
import requests
import json
from datetime import datetime
from coordinator.services.broker import subscribe as broker_subscribe_service


def update_brokers_subscription_plan():
    response_code, all_subscriptions = broker_subscribe_service.get_all_subscriptions()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")
    for broker_url in all_subscriptions.keys():
        data = all_subscriptions[broker_url]
        response = requests.post(f"http://{broker_url}/subscription-plan",
                                 data=json.dumps({"subscription_plans": data}))
        if response.status_code != 200:
            print(f"Error during sending subscription to broker #{broker_url}")


def check_heartbeat():
    response = requests.get('http://127.0.0.1:5001/client/list_all_heartbeats')
    data = response.json()

    if len(data) == 0:
        return
    for key in data.keys():
        datetime_seconds = float(data[key])
        diff_seconds = datetime.now().timestamp() - datetime_seconds
        if diff_seconds > 30:
            requests.post("http://127.0.0.1:5001/client/delete_heartbeat", data=json.dumps({"client_url": key}))
            requests.post("http://127.0.0.1:5001/subscribe/delete", data=json.dumps({"client_url": key}))
            # update_brokers_subscription_plan()


def run_check_heartbeat_job():
    check_heartbeat()
    threading.Timer(10, run_check_heartbeat_job).start()
