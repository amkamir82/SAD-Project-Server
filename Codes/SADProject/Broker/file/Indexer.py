import json
import os
import threading


class Indexer(object):
    def __init__(self, partition: str):
        self.partition = partition
        self._write = 0
        self._read = 0
        self._sync = 0
        self._write_lock = threading.Lock()
        self._read_lock = threading.Lock()
        self._sync_lock = threading.Lock()
        self.load()

    def save(self):
        with self._write_lock, self._read_lock, self._sync_lock:
            dictionary = {'partition': self.partition, 'write': self._write, 'read': self._read, 'sync': self._sync}

            file_path = self.__path()
            with open(file_path, 'w') as file:
                json.dump(dictionary, file)

            print(f'Indexes saved to {file_path}')

    def load(self):
        with self._write_lock, self._read_lock, self._sync_lock:
            file_path = self.__path()

            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    loaded_dict = json.load(file)

                self._write = loaded_dict['write']
                self._read = loaded_dict['read']
                self._sync = loaded_dict['sync']

                print(
                    f'Dictionary loaded from {file_path}, write: {self._write}, read: {self._read}, sync: {self._sync}')
            else:
                print(f'File {file_path} does not exist. Setting to zero all indexes')

    def inc_write(self):
        with self._write_lock:
            self._write += 1

    def get_write(self):
        return self._write

    def inc_read(self):
        with self._read_lock:
            self._read += 1

    def get_read(self):
        return self._read

    def inc_sync(self):
        with self._sync_lock:
            self._sync += 1

    def get_sync(self):
        return self._sync

    def __path(self):
        current_working_directory = os.getcwd()
        return os.path.join(current_working_directory, 'indexes.json', self.partition)
