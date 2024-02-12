import threading
import time

import schedule

import sys, os
BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from file.read import Read
from manager.env import *

fetch_lock = threading.Lock()
def read_sample_data():
    with fetch_lock:
        read_instance = Read(get_primary_partition(), get_replica_url())
        # print(id(read_instance))
        # print(threading.currentThread().ident)

        print("reading sample data")
        print(read_instance.read_data())
        # TODO: send to subscriber


def schedule_read():
    while True:
        read_sample_data()
        time.sleep(1)