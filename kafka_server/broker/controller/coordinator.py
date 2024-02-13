import os
import time
import psutil
import requests


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
        response = requests.options(_master_coordinator_url(), timeout=1)
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
        response = requests.post(coordinator, json=data, timeout=2)
        return response.status_code == 200
    except requests.RequestException as e:
        print(f"Error on heartbeat {e}")

    return False


def heartbeat():
    heartbeat_url = 'broker/heartbeat'

    while True:
        cpu_usage = psutil.cpu_percent()
        disk_usage = psutil.disk_usage('/').percent

        payload = {
            'cpu_usage': cpu_usage,
            'disk_usage': disk_usage
        }

        if _post(payload, heartbeat_url):
            print(f"Heartbeat {payload}")
        else:
            print("Heartbeat Failed")

        time.sleep(3)  # 3 Seconds wait for another heartbeat
