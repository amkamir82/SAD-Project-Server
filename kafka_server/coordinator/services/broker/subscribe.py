from datetime import datetime
import json
import threading
import requests

from coordinator.services.broker import database as broker_database
from coordinator.services.client import database as client_database


def get_all_subscriptions():
    r = requests.get("http://127.0.0.1:5001/subscribe/list_all", timeout=2)
    return r.status_code, r.json()


def write_subscriptions(subscriptions):
    r = requests.post("http://127.0.0.1:5001//subscribe/write_subscriptions",
                      data=json.dumps({"subscriptions": subscriptions}), timeout=2)
    return r.status_code


def send_subscribe_to_broker(broker_url, data):
    r = requests.post(
        f"{broker_url}/subscribers",
        data=json.dumps({"subscribers": data}),
        timeout=2,
    )
    return r.status_code


def update_brokers_subscriptions():
    print("######update brokers subscriptions")
    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")

    response_code, all_clients = client_database.list_all_clients()
    if response_code != 200:
        raise Exception("Error during getting list of clients from database")

    try:
        print("########all brokers\n", all_brokers)
        for client_url in all_clients:
            r = requests.post(
                f"{client_url}/update-brokers",
                data=json.dumps({"brokers": all_brokers}),
                timeout=2,
            )
            if r.status_code != 200:
                print("ey vayyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")

        for broker_id in all_brokers.keys():
            requests.post(f"{all_brokers[broker_id]}/subscription", data=json.dumps({"brokers": all_brokers}),
                          headers={"Content-Type": "application/json"}, timeout=2)
    except Exception as e:
        print(str(e))


def prepare_updating(all_brokers, down_broker_id, down_broker_url):
    response_code, all_brokers_replicas = broker_database.list_of_replicas()
    if response_code != 200:
        raise Exception("Error during getting list of brokers replicas")

    # find replica of a broker which is on down broker
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
    for broker_id in all_brokers_replicas.keys():
        print("hi")
        print("a ", all_brokers_replicas[broker_id])
        print("b ", down_broker_url)
        if all_brokers_replicas[broker_id] == down_broker_url:
            print(broker_id)
            print("hi2")
            update_replica_partition_of_a_broker_which_is_in_down_broker(all_brokers[broker_id])
    print("@@@@@@@@@@@@@@@@@@@@@@")

    # update all brokers
    update_brokers_subscriptions()

    # update new subscription plans
    update_subscribers()

    # find replica of down broker
    down_broker_replica_url = all_brokers_replicas[down_broker_id]
    update_replica_partition_of_a_down_broker(down_broker_id, down_broker_replica_url)


def update_replica_partition_of_a_broker_which_is_in_down_broker(broker_url):
    print(f"###############sedning request to {broker_url} to aware its replica")
    r = requests.post(f"{broker_url}/replica/down", timeout=2)
    print(r.status_code)
    if r.status_code != 200:
        raise Exception("Error in sending request to broker to tell it its replica is down")


def update_replica_partition_of_a_down_broker(down_broker_id, down_broker_replica_url):
    print(f"###############sedning request to sync replica of down broker# {down_broker_id}:{down_broker_replica_url}")
    r = requests.post(f"{down_broker_replica_url}/broker/down", data=json.dumps({"partition": down_broker_id}))
    print(r.status_code)
    if r.status_code != 200:
        raise Exception("Error in sending request to broker which has the replica of a down broker")


def update_brokers_list(broker_url):
    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")
    down_broker_id = None
    for broker_id in all_brokers.keys():
        data = all_brokers[broker_id]
        if broker_url == data:
            down_broker_id = broker_id
            print(f"########################sending request to delete broker {broker_id}:{broker_url}")
            response = requests.post(
                "http://127.0.0.1:5001/broker/delete",
                data=json.dumps({"broker_id": broker_id}),
                timeout=2,
            )
            if response.status_code != 200:
                print(f"Error during sending subscription to broker #{broker_url}")
    prepare_updating(all_brokers, down_broker_id, broker_url)
    update_brokers_subscriptions()


def update_subscribers():
    print("#########3updating subscribers after broker down")
    response_code, all_subscriptions = get_all_subscriptions()
    print("################allllll subscriptions\n", all_subscriptions)
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")

    response_code, all_brokers = broker_database.list_all_brokers()
    if response_code != 200:
        raise Exception("Error during getting list of brokers from database")

    all_subscribers = []
    for broker_id_url in all_subscriptions.keys():
        for sub in all_subscriptions[broker_id_url]:
            all_subscribers.append(sub)

    print("######all subscriber before\n", all_subscribers)
    unique_tuples = set(tuple(x) for x in all_subscribers)
    all_subscribers = [list(x) for x in unique_tuples]

    all_subscribers = list(all_subscribers)
    tmp_subscriptions = {}
    all_subscribers_length = len(all_subscribers)
    all_brokers_length = len(all_brokers)
    print("######all subscribers\n", all_subscribers)
    ex = all_subscribers_length // all_brokers_length
    print("###########ex ", ex)
    j = 0
    for broker_id in all_brokers:
        i = ex
        while i > 0:
            if f"{broker_id}:{all_brokers[broker_id]}" not in tmp_subscriptions:
                tmp_subscriptions[f"{broker_id}:{all_brokers[broker_id]}"] = []
            tmp_subscriptions[f"{broker_id}:{all_brokers[broker_id]}"].append(all_subscribers[j])
            j += 1
            i -= 1

    print("####aghaei\n", tmp_subscriptions)

    for index in range(j, len(all_subscribers)):
        for broker_id in all_brokers:
            if f"{broker_id}:{all_brokers[broker_id]}" not in tmp_subscriptions:
                tmp_subscriptions[f"{broker_id}:{all_brokers[broker_id]}"] = []
            tmp_subscriptions[f"{broker_id}:{all_brokers[broker_id]}"].append(all_subscribers[index])

    print("####aghaei2\n", tmp_subscriptions)

    for broker_id in all_brokers.keys():
        t = {}
        for sub in tmp_subscriptions[f"{broker_id}:{all_brokers[broker_id]}"]:
            t[sub[1]] = sub[0]
        print("##########senda subscriptions to broker")
        print(t)
        send_subscribe_to_broker(all_brokers[broker_id], t)

    print("@@@@@@@@tmp_subscriptions to write\n", tmp_subscriptions)
    response_code = write_subscriptions(tmp_subscriptions)
    if response_code != 200:
        raise Exception("Error during finding broker for subscribe")


def check_heartbeat():
    try:
        response = requests.get('http://127.0.0.1:5001/broker/list_all_heartbeats', timeout=2)
        data = response.json()

        if len(data) == 0:
            return
        for key in data.keys():
            datetime_seconds = float(data[key])
            diff_seconds = datetime.now().timestamp() - datetime_seconds
            if diff_seconds > 15:
                requests.post(
                    "http://127.0.0.1:5001/broker/delete_heartbeat",
                    data=json.dumps({"broker_url": key}),
                    timeout=2,
                )
                update_brokers_list(key)
    except Exception as e:
        print(str(e))


def run_check_heartbeat_job():
    check_heartbeat()
    threading.Timer(10, run_check_heartbeat_job).start()
