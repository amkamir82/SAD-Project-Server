import os
import sys
import threading
import time

from file.sync import Sync
from manager.env import is_replica_mirror_down, get_replica_mirror_down_partition

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))
sync_lock = threading.Lock()


def sync_replica_data():
    if not is_replica_mirror_down():
        return

    with sync_lock:
        sync_instance = Sync(get_replica_mirror_down_partition(), None)

        print("sync replica data")
        print(sync_instance.sync_data())


def schedule_sync_replica():
    while True:
        sync_replica_data()
        time.sleep(1)
