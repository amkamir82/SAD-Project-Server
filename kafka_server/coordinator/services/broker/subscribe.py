from datetime import datetime
import json
import threading
import requests

from coordinator.services.broker import database as broker_database
from coordinator.services.client import database as client_database


def get_all_subscriptions():
    r = requests.get("http://127.0.0.1:5001/subscribe/list_all", timeout=2)
    return r.status_code, r.json()


def send_subscribe_to_broker(broker_url, data):
    r = requests.post(
        f"{broker_url}/subscribers",
        data=json.dumps({"subscribers": data}),
        timeout=2,
    )
    return r.status_code


def update_brokers_subscriptions():
    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")

    response_code, all_clients = client_database.list_all_clients()
    if response_code != 200:
        raise Exception("Error during getting list of clients from database")

    for client_url in all_clients:
        requests.post(
            f"{client_url}/subscription",
            data=json.dumps({"brokers": all_brokers}),
            timeout=2,
        )

    for broker_id in all_brokers.keys():
        requests.post(f"{all_brokers[broker_id]}/subscription", data=json.dumps({"brokers": all_brokers}),
                      headers={"Content-Type": "application/json"}, timeout=2)


def prepare_updating(all_brokers, down_broker_id, down_broker_url):
    response_code, all_brokers_replicas = broker_database.list_of_replicas()
    if response_code != 200:
        raise Exception("Error during getting list of brokers replicas")

    # find replica of a broker which is on down broker
    for broker_id in all_brokers_replicas.keys():
        if all_brokers_replicas[broker_id] == down_broker_url:
            update_replica_partition_of_a_broker_which_is_in_down_broker(all_brokers[broker_id])

    # update all brokers
    update_brokers_subscriptions()

    # find replica of down broker
    down_broker_replica_url = all_brokers_replicas[down_broker_id]
    update_replica_partition_of_a_down_broker(down_broker_id, down_broker_replica_url)


def update_replica_partition_of_a_broker_which_is_in_down_broker(broker_url):
    r = requests.get(f"{broker_url}/replica/down", timeout=2)
    if r.status_code != 200:
        raise Exception("Error in sending request to broker to tell it its replica is down")


def update_replica_partition_of_a_down_broker(down_broker_id, down_broker_replica_url):
    r = requests.post(f"{down_broker_replica_url}/broker/down",
                      data=json.dumps({"partition": down_broker_id}),
                      timeout=2)
    if r.status_code != 200:
        raise Exception("Error in sending request to broker which has the replica of a down broker")


def update_brokers_list(broker_url):
    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")
    for broker_id in all_brokers.keys():
        data = all_brokers[broker_id]
        if broker_url == data:
            response = requests.post(
                "http://127.0.0.1:5001/broker/delete",
                data=json.dumps({"broker_id": broker_id}),
                timeout=2,
            )
            if response.status_code != 200:
                print(f"Error during sending subscription to broker #{broker_url}")
        prepare_updating(all_brokers, broker_id, broker_url)
        update_brokers_subscriptions()


def check_heartbeat():
    response = requests.get('http://127.0.0.1:5001/broker/list_all_heartbeats', timeout=2)
    data = response.json()

    if len(data) == 0:
        return
    for key in data.keys():
        datetime_seconds = float(data[key])
        diff_seconds = datetime.now().timestamp() - datetime_seconds
        if diff_seconds > 30:
            requests.post(
                "http://127.0.0.1:5001/broker/delete_heartbeat",
                data=json.dumps({"broker_url": key}),
                timeout=2,
            )
            update_brokers_list(key)


def run_check_heartbeat_job():
    check_heartbeat()
    threading.Timer(10, run_check_heartbeat_job).start()
