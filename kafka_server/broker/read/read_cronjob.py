import os
import sys
import threading
import time
from file.read import Read
from manager.env import get_primary_partition, get_replica_url

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))
fetch_lock = threading.Lock()


def read_sample_data():
    with fetch_lock:
        read_instance = Read(get_primary_partition(), get_replica_url())

        print("reading sample data")
        print(read_instance.read_data())


def schedule_read():
    while True:
        read_sample_data()
        time.sleep(1)
