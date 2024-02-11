import json
import os
import requests
import threading

from Codes.SADProject.Broker.file.hash import hash_md5
from Codes.SADProject.Broker.file.segment import Segment
from Codes.SADProject.Broker.manager.partitionManager import PartitionManager


class Read(object):
    _instances_lock = threading.Lock()
    _read_lock = threading.Lock()
    _instances = {}

    def __new__(cls, partition: str, replica: str):
        with cls._instances_lock:
            if partition not in cls._instances:
                cls._instances[partition] = super().__new__(cls)
            return cls._instances[partition]

    def __init__(self, partition: str, replica: str):
        if not hasattr(self, 'initialized'):
            self.partition = partition
            self.segment = Segment(partition, replica)
            self.partitionManager = PartitionManager(partition)
            self.subscribers = self.get_subscribers()
            self.initialized = True

    def read_data(self):
        with self._read_lock:
            if self.segment.get_read_index() >= self.segment.get_write_index():
                print("No key found")
                return None, None

            key, value = self.segment.read()
            if key is None:
                print("No key found")
                return None, None

            md5 = hash_md5(key)
            partition_count = self.partitionManager.partition_count()
            if int(md5, 16) % partition_count != int(self.partition) - 1:
                self.segment.approve_reading()
                return self.read_data()

            sent = self.send_to_subscriber(key, value)
            if sent:
                self.segment.approve_reading()

            return sent

    def pull_data(self):
        pass

    @staticmethod
    def get_subscribers():
        subscriptions_file_path = os.path.join(os.getcwd(), '../data', 'subscriptions', 'subscribers.json')

        with open(subscriptions_file_path, 'r') as file:
            subscribers = json.load(file)

        if len(subscribers) == 0:
            raise Exception('No subscribers found')
        return subscribers

    def send_to_subscriber(self, key: str, value: str) -> bool:
        chosen_subscriber = self.choose_subscriber()
        url = f'{chosen_subscriber}/subscribe'

        try:
            response = requests.post(url, json={'key': key, 'value': value})
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
        return self.subscribers[chosen_key]
