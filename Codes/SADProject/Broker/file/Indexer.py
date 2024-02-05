import json
import os
import threading


class Indexer(object):
    _write_lock = threading.Lock()
    _read_lock = threading.Lock()
    _sync_lock = threading.Lock()

    def __init__(self, partition: str):
        self.partition = partition
        self._write = 0
        self._read = 0
        self._sync = 0
        self.load()
        path = self.__dir_path()
        os.makedirs(path, exist_ok=True)

    def save(self):
        self._save_variable(self._write, 'write')
        self._save_variable(self._read, 'read')
        self._save_variable(self._sync, 'sync')

    def load(self):
        self._write = self._load_variable('write')
        self._read = self._load_variable('read')
        self._sync = self._load_variable('sync')

        print(f'indexes loaded, write: {self._write}, read: {self._read}, sync: {self._sync}')

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

    def get_read(self):
        return self._read

    def inc_sync(self):
        with self._sync_lock:
            self._sync += 1
            self._save_variable(self._sync, 'sync')

    def get_sync(self):
        return self._sync

    def _save_variable(self, value, action):
        file_path = self.__path(action)
        with open(file_path, 'w+') as file:
            json.dump(value, file)

        print(f'{action} index saved to {file_path}')

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
        return os.path.join(current_working_directory, f'partition_index_{self.partition}', f'{action}_index')

    def __dir_path(self) -> str:
        current_working_directory = os.getcwd()
        return os.path.join(current_working_directory, f'partition_index_{self.partition}')
