import json
import logging
import os
import time
import psutil
import requests

logger = logging.getLogger(__name__)


def _master_coordinator_url() -> str:
    return os.getenv('MASTER_COORDINATOR_URL', '')


def _replica_coordinator_url() -> str:
    return os.getenv('REPLICA_COORDINATOR_URL', '')


def _url(master_not_replica: bool, url: str) -> str:
    if master_not_replica:
        return os.path.join(_master_coordinator_url(), url)
    return os.path.join(_replica_coordinator_url(), url)


def _check_if_master_alive():
    try:
        response = requests.get(_master_coordinator_url(), timeout=1)
        if response.status_code == 200:
            return True
        print(f"Unexpected response: {response.status_code}")
        return False
    except requests.RequestException as e:
        print(f"Error checking heartbeat server: {e}")

    return False


def _post(data, url: str):
    master_alive = _check_if_master_alive()
    coordinator = _url(master_not_replica=master_alive, url=url)
    if not master_alive:
        print(f"master coordinator {_master_coordinator_url()} is not alive")

    try:
        response = requests.post(coordinator, json=data, data=json.dumps(data), timeout=2)
        return response.status_code == 200
    except requests.RequestException as e:
        print(f"Error on post {e}")


def _get(data, url: str):
    for i in range(3):
        try:
            master_alive = _check_if_master_alive()
            coordinator = _url(master_not_replica=master_alive, url=url)
            if not master_alive:
                print(f"master coordinator {_master_coordinator_url()} is not alive")
            response = requests.get(coordinator, json=data, data=json.dumps(data), timeout=2)
            if response.status_code == 200:
                return json.loads(response.content.decode("utf-8"))
        except requests.RequestException as e:
            logger.error(e)
            print(f"Error on {i}")

    return False


def heartbeat():
    heartbeat_url = 'broker/heartbeat'

    while True:
        cpu_usage = psutil.cpu_percent()
        disk_usage = psutil.disk_usage('/').percent

        payload = {
            'cpu_usage': cpu_usage,
            'disk_usage': disk_usage,
            'ip': os.getenv("IP"),
            'port': os.getenv("PORT"),
        }
        _post(payload, heartbeat_url)
        time.sleep(3)  # 3 Seconds wait for another heartbeat


def init_from_coordinator():
    init_url = 'broker/init'

    partition = os.getenv("PRIMARY_PARTITION")
    port = os.getenv("PORT")

    payload = {
        'broker_id': partition,
        'port': port,
        'ip': os.getenv("IP")
    }

    return _get(data=payload, url=init_url)
