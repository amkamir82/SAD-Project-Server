import json
import os

import requests
import threading

import sys

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from file.hash import hash_md5
from file.segment import Segment
from manager.env import get_partition_count


class Sync(object):
    _instances_lock = threading.Lock()
    _sync_lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instances_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, partition: str, replica: str):
        if not hasattr(self, 'initialized'):
            self.partition = partition
            self.segment = Segment(partition, replica)
            self.brokers = self.get_brokers()
            self.initialized = True

    def sync_data(self):
        if not self.check_data_exist():
            return None, None

        with self._sync_lock:
            key, value = self.segment.read()
            md5 = hash_md5(key)
            partition_count = get_partition_count()
            broker_id = int(md5, 16) % partition_count
            if broker_id == int(self.partition) - 1:
                self.segment.approve_sync()
                return self.sync_data()

            sent = self.send_to_broker(key, value, broker_id)
            if sent:
                self.segment.approve_sync()

            return sent

    def check_data_exist(self):
        if self.segment.get_sync_index() >= self.segment.get_write_index():
            print(f"No key found {self.segment.get_sync_index()} in {self.segment.get_write_index()}")
            return False

        key, value = self.segment.read()
        if key is None:
            print("No key found")
            return False

        return True

    @staticmethod
    def get_brokers():
        brokers_file_path = os.path.join(os.getcwd(), 'data', 'subscriptions', 'brokers.json')

        with open(brokers_file_path, 'r') as file:
            brokers = json.load(file)

        if len(brokers) == 0:
            raise Exception('No brokers found')
        return brokers

    def send_to_broker(self, key: str, value: str, broker_id: int) -> bool:
        url = f'{self.brokers[broker_id]}/write'
        print(f"sync {key} to {url}", flush=True)

        try:
            response = requests.post(url, json={'key': key, 'value': value})
            return response.status_code == 200
        except Exception as e:
            print(e)
            return False
