import json
import os
import requests
import threading


class Indexer(object):
    _instances = {}
    _lock = threading.Lock()
    _write_lock = threading.Lock()
    _read_lock = threading.Lock()
    _sync_lock = threading.Lock()

    def __new__(cls, partition: str, replica: str):
        with cls._lock:
            if partition not in cls._instances:
                cls._instances[partition] = super().__new__(cls)
                cls._instances[partition].partition = partition
                cls._instances[partition].replica = replica
                cls._instances[partition]._write = 0
                cls._instances[partition]._read = 0
                cls._instances[partition]._sync = 0
                cls._instances[partition].load()
                path = cls._instances[partition].__dir_path()
                os.makedirs(path, exist_ok=True)
            return cls._instances[partition]

    def load(self):
        self._write = self._load_variable('write')
        self._read = self._load_variable('read')
        self._sync = self._load_variable('sync')

        print(f'Indexes loaded, write: {self._write}, read: {self._read}, sync: {self._sync}')

    def inc_write(self):
        with self._write_lock:
            self._write += 1
            self._save_variable(self._write, 'write')

    def get_write(self):
        return self._write

    def inc_read(self):
        with self._read_lock:
            self._read += 1
            self._save_variable(self._read, 'read')
            self.send_to_replica()

    def get_read(self):
        return self._read

    def inc_sync(self):
        with self._sync_lock:
            self._sync += 1
            self._save_variable(self._sync, 'sync')
            self.send_to_replica()

    def get_sync(self):
        return self._sync

    def update_read_sync(self, read: int, sync: int):
        if read != self._read:
            with self._read_lock:
                self._read = read
                self._save_variable(read, 'read')

        if sync != self._sync:
            with self._sync_lock:
                self._sync = sync
                self._save_variable(sync, 'sync')

    def _save_variable(self, value, action):
        file_path = self.__path(action)
        with open(file_path, 'w+') as file:
            json.dump(value, file)

    def _load_variable(self, action):
        file_path = self.__path(action)

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                loaded_value = json.load(file)

            return loaded_value
        else:
            print(f'File {file_path} does not exist. Returning 0 for {action}')
            return 0

    def __path(self, action: str) -> str:
        current_working_directory = os.getcwd()
        return os.path.join(
            current_working_directory,
            '../data',
            'partition_index',
            f'{self.partition}',
            f'{action}_index'
        )

    def __dir_path(self) -> str:
        current_working_directory = os.getcwd()
        return os.path.join(
            current_working_directory,
            '../data',
            'partition_index',
            f'{self.partition}'
        )

    def send_to_replica(self):
        url = f'{self.replica}/replica/index'
        data = {'partition': self.partition, 'read': self._read, 'sync': self._sync}
        response = requests.post(url, json=data)
        if response.status_code == 200:
            raise Exception(f'indexed not yet updated {response}')
