import threading
import time
import schedule

import os, sys

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from read.read_cronjob import schedule_read
from controller.coordinator import heartbeat
from read.read_cronjob import *

def schedule_read_thread():
    schedule_read()
    while True:
        schedule.run_pending()
        time.sleep(1)


def send_heartbeat():
    heartbeat()


read_thread = threading.Thread(target=schedule_read_thread)
read_thread.daemon = True
read_thread.start()

heartbeat_thread = threading.Thread(target=send_heartbeat)
heartbeat_thread.daemon = True
heartbeat_thread.start()