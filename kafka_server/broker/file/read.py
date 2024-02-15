from datetime import datetime, timedelta
import json
import os
import sys
import threading
import time

import requests

from file.hash import hash_md5
from file.segment import Segment
from manager.env import get_partition_count


BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))


class Read:
    _instances_lock = threading.Lock()
    _read_lock = threading.Lock()
    _instances = {}

    def __new__(cls, partition: str, replica: str):
        if f"{partition}-{replica}" not in cls._instances[f"{partition}-{replica}"]:
            with cls._instances_lock:
                cls._instances[f"{partition}-{replica}"] = super().__new__(cls)

        return cls._instances[f"{partition}-{replica}"]

    def __init__(self, partition: str, replica: str):
        self.subscribers = None
        self.partition = partition
        self.message_in_fly = False
        self.message_in_fly_since = datetime.now()
        self.segment = Segment(partition, replica)

        self.toggle_thread = threading.Thread(target=self.toggle_message_in_fly)
        self.toggle_thread.daemon = True
        self.toggle_thread.start()

    def toggle_message_in_fly(self):
        while True:
            time_diff = datetime.now() - self.message_in_fly_since
            if self.message_in_fly and time_diff > timedelta(seconds=2):
                self.message_in_fly = False
                self.save_message_in_fly()

            time.sleep(5)

    def read_data(self):
        self.subscribers = self.get_subscribers()
        if len(self.subscribers) == 0:
            print("No subscribers")
            return None, None
        self.load_message_in_fly()
        if self.message_in_fly:
            print("there is message in fly", flush=True)
            return None, None
        if not self.check_data_exist():
            return None, None

        with self._read_lock:
            key, value = self.segment.read()
            md5 = hash_md5(key)
            partition_count = get_partition_count()
            if int(md5, 16) % partition_count != int(self.partition) - 1:
                print("data for other partition", flush=True)
                self.segment.approve_reading()
                return self.read_data()

            self.message_in_fly = True
            self.save_message_in_fly()

            sent = self.send_to_subscriber(key, value)
            if sent:
                self.segment.approve_reading()

            self.message_in_fly = False
            self.save_message_in_fly()

            return sent

    def pull_data(self):
        self.load_message_in_fly()
        if self.message_in_fly:
            print("there is message in fly")
            return None, None
        if not self.check_data_exist():
            return None, None

        with self._read_lock:
            key, value = self.segment.read()

            md5 = hash_md5(key)
            partition_count = get_partition_count()
            if int(md5, 16) % partition_count != int(self.partition) - 1:
                self.segment.approve_reading()
                return self.pull_data()

            self.message_in_fly = True
            self.save_message_in_fly()
            return key, value

    def ack_message(self):
        if self.message_in_fly:
            with self._read_lock:
                self.segment.approve_reading()
                self.message_in_fly = False
                self.save_message_in_fly()

    def check_data_exist(self):
        if self.segment.get_read_index() >= self.segment.get_write_index():
            print(f"No key found {self.segment.get_read_index()} "
                  f"in {self.segment.get_write_index()}")
            return False

        key, _ = self.segment.read()
        if key is None:
            print("No key found")
            return False

        return True

    @staticmethod
    def get_subscribers():
        subscriptions_file_path = os.path.join(
            os.getcwd(),
            'data',
            'subscriptions',
            'subscribers.json'
        )
        if not os.path.exists(subscriptions_file_path):
            print("No subscriptions file found")
            return []

        with open(subscriptions_file_path, 'r', encoding='utf8') as f:
            subscribers = json.load(f)
            print("Found {} subscribers".format(subscribers))

        return subscribers

    def send_to_subscriber(self, key: str, value: str) -> bool:
        subscriber_id, chosen_subscriber = self.choose_subscriber()
        url = f'{chosen_subscriber}/subscribe-{subscriber_id}'
        print(f"Sending {key} to {url}", flush=True)

        try:
            response = requests.post(url, json={'key': key, 'value': value}, timeout=2)
            if response.status_code != 200:
                print(response.json(), response.content, response.status_code)

            return response.status_code == 200
        except Exception as e:
            print(e)
            return False

    def choose_subscriber(self):
        read_index = self.segment.get_read_index()
        subscriber_count = len(self.subscribers)
        id_to_key = {}
        for i, key in enumerate(self.subscribers.keys()):
            id_to_key[i] = key

        chosen_key = id_to_key[read_index % subscriber_count]
        return chosen_key, self.subscribers[chosen_key]

    def load_message_in_fly(self):
        message_file_path = os.path.join(os.getcwd(), 'data', 'message_in_fly.json')
        if os.path.exists(message_file_path):
            with open(message_file_path, 'r', encoding='utf8') as f:
                data = json.load(f)
                self.message_in_fly = data.get('message_in_fly', False)
                self.message_in_fly_since = datetime.fromisoformat(
                    data.get('message_in_fly_since', datetime.now().isoformat()))

    def save_message_in_fly(self):
        message_file_path = os.path.join(os.getcwd(), 'data', 'message_in_fly.json')
        data = {
            'message_in_fly': self.message_in_fly,
            'message_in_fly_since': self.message_in_fly_since.isoformat()
        }
        with open(message_file_path, 'w+', encoding='utf8') as f:
            json.dump(data, f)
