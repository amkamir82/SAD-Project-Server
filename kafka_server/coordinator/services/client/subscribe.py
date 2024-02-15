from datetime import datetime
import json
import threading
import requests

from coordinator.services.broker import subscribe as broker_subscribe_service


def update_brokers_subscription_plan():
    response_code, all_subscriptions = broker_subscribe_service.get_all_subscriptions()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")
    for broker_url in all_subscriptions.keys():
        data = all_subscriptions[broker_url]
        t = {}
        for sub in data:
            t[sub[0]] = sub[1]

        response = requests.post(
            f"{broker_url[2:]}/subscribers",
            data=json.dumps({"subscribers": data}),
            timeout=2,
        )
        if response.status_code != 200:
            print(f"Error during sending subscription to broker #{broker_url}")


def check_heartbeat():
    try:
        print("##############checing heartbeats")
        response = requests.get(
            'http://127.0.0.1:5001/client/list_all_heartbeats',
            timeout=2,
        )
        data = response.json()
        print(data)
        if len(data) == 0:
            return
        for key in data.keys():
            datetime_seconds = float(data[key])
            diff_seconds = datetime.now().timestamp() - datetime_seconds
            if diff_seconds > 30:
                print("##############delete client heartbeat")
                requests.post(
                    "http://127.0.0.1:5001/client/delete_heartbeat",
                    data=json.dumps({"client_url": key}),
                    timeout=2,
                )

                print("##############delete all subscriptions for client")
                requests.post(
                    "http://127.0.0.1:5001/subscribe/delete",
                    data=json.dumps({"client_url": key}),
                    timeout=2,
                )
                update_brokers_subscription_plan()
    except Exception as e:
        print(str(e))


def run_check_heartbeat_job():
    check_heartbeat()
    threading.Timer(10, run_check_heartbeat_job).start()
