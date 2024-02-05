import os
import threading

from Codes.SADProject.Broker.file.Indexer import Indexer

SEGMENT_SIZE = 100


class Segment(object):
    _instances = {}
    _instances_lock = threading.Lock()
    _append_lock = threading.Lock()

    def __new__(cls, partition: str):
        with cls._instances_lock:
            if partition not in cls._instances:
                cls._instances[partition] = super(Segment, cls).__new__(cls)
                cls._instances[partition].partition = partition
                cls._instances[partition].indexer = Indexer(partition)
            return cls._instances[partition]

    def append(self, key: str, value: bytes):
        with self._append_lock:
            segment_path = self.write_segment_path()
            if self.indexer.get_write() % SEGMENT_SIZE == 0:
                os.makedirs(segment_path, exist_ok=True)

            data_file_path = os.path.join(segment_path, f'{self.indexer.get_write()}.dat')
            kb = 1024

            with open(data_file_path, 'wb+') as entry_file:
                entry_file.write(f'{key}: '.encode('utf-8'))
                for i in range(0, len(value), kb):
                    chunk = value[i:i + kb]
                    entry_file.write(chunk)

            self.indexer.inc_write()

    def write_segment_path(self):
        segment_number = self.write_segment_number()
        return self.__path(segment_number)

    def read_segment_path(self):
        segment_number = self.read_segment_number()
        return self.__path(segment_number)

    def write_segment_number(self):
        write_index = self.indexer.get_write()
        return write_index // SEGMENT_SIZE + 1

    def read_segment_number(self):
        read_index = self.indexer.get_read()
        return read_index // SEGMENT_SIZE + 1

    def __path(self, segment_number: int) -> str:
        current_working_directory = os.getcwd()
        return os.path.join(current_working_directory, f'partition_data_{self.partition}', f'segment_{segment_number}')