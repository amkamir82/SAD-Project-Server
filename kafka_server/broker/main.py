import threading
import schedule
import os, sys

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from read.read_cronjob import schedule_read
from read.sync_cronjob import schedule_sync
from controller.coordinator import heartbeat

def schedule_read_thread():
    schedule_read()

def schedule_sync_thread():
    schedule_sync()

def send_heartbeat():
    heartbeat()


def init():
    read_thread = threading.Thread(target=schedule_read_thread)
    read_thread.daemon = True
    read_thread.start()

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    sync_thread = threading.Thread(target=schedule_sync_thread)
    sync_thread.daemon = True
    sync_thread.start()

    schedule.run_pending()