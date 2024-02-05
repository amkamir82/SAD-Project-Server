from Codes.SADProject.Broker.file.segment import Segment
from Codes.SADProject.Broker.file.hash import hash_md5
from Codes.SADProject.Broker.manager.partitionManager import PartitionManager


class Write(object):
    def __init__(self, partition: str):
        self.partition = partition
        self.segment = Segment(partition)
        self.partitionManager = PartitionManager(partition)

    def write_data(self, key: str, value: bytes):
        md5 = hash_md5(key)
        partition_count = self.partitionManager.partition_count()
        if int(md5, 16) % partition_count != int(self.partition) - 1:
            raise Exception("invalid data")

        self.segment.append(key, value)
        print("done")
        # todo: send to replica
