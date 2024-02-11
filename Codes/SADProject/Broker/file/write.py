import threading
import requests

from Codes.SADProject.Broker.file.segment import Segment
from Codes.SADProject.Broker.file.hash import hash_md5
from Codes.SADProject.Broker.manager.partitionManager import PartitionManager


class Write(object):
    _instances = {}
    _write_lock = threading.Lock()

    def __new__(cls, partition: str, replica: str):
        if partition not in cls._instances:
            cls._instances[partition] = super(Write, cls).__new__(cls)
        return cls._instances[partition]

    def __init__(self, partition: str, replica: str):
        if hasattr(self, 'initialized'):
            return
        self.initialized = True

        self.partition = partition
        self.segment = Segment(partition, replica)
        self.partitionManager = PartitionManager(partition)
        self.replica = replica

    def write_data(self, key: str, value: bytes):
        with self._write_lock:
            md5 = hash_md5(key)
            partition_count = self.partitionManager.partition_count()
            if int(md5, 16) % partition_count != int(self.partition) - 1:
                raise Exception("key is not for this partition")

            appended = self.segment.append(key, value)
            if not appended:
                return False

            replicated = self.send_to_replica(key, value)
            if not replicated:
                return False

            return self.segment.approve_appending()

    def replicate_data(self, key: str, value: bytes):
        if self.segment.append(key, value):
            return self.segment.approve_appending()

    def send_to_replica(self, key: str, value: bytes) -> bool:
        url = f'{self.replica}/replica/data'
        value_str = value.decode('utf-8')
        response = requests.post(url, json={'key': key, 'value': value_str})
        return response.status_code == 200
