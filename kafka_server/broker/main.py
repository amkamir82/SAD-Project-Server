import os
import schedule
import sys
import threading

from controller.coordinator import heartbeat, init_from_coordinator
from read.read_cronjob import schedule_read
from read.replica_sync_cronjob import schedule_sync_replica
from read.sync_cronjob import schedule_sync


BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))


def schedule_read_thread():
    schedule_read()


def schedule_sync_thread():
    schedule_sync()


def send_heartbeat():
    heartbeat()


def sync_replica():
    schedule_sync_replica()


def init_broker():
    data = init_from_coordinator()
    os.environ["REPLICA_URL"] = data['replica_url']
    os.environ["MASTER_COORDINATOR_URL"] = data['master_coordinator_url']
    os.environ["REPLICA_COORDINATOR_URL"] = data['replica_coordinator_url']
    os.environ["PARTITION_COUNT"] = str(data['partition_count'])


def init():
    os.makedirs(os.path.join(os.getcwd(), '../data'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), '../data', 'subscriptions'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), '../data', 'partition_data'), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), '../data', 'partition_index'), exist_ok=True)
    init_broker()
    read_thread = threading.Thread(target=schedule_read_thread)
    read_thread.daemon = True
    read_thread.start()

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    # sync_thread = threading.Thread(target=schedule_sync_thread)
    # sync_thread.daemon = True
    # sync_thread.start()

    schedule.run_pending()
