import os
import sys
import threading
import time

from file.sync import Sync
from manager.env import get_primary_partition, get_replica_url

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))
sync_lock = threading.Lock()


def sync_sample_data():
    with sync_lock:
        sync_instance = Sync(get_primary_partition(), get_replica_url())

        print("sync sample data")
        print(sync_instance.sync_data())


def schedule_sync():
    while True:
        sync_sample_data()
        time.sleep(1)
