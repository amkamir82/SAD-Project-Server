from Codes.SADProject.Broker.file.segment import Segment
from Codes.SADProject.Broker.manager.partitionManager import PartitionManager


class Read(object):
    def __init__(self, partition: str):
        self.partition = partition
        self.segment = Segment(partition)
        self.partitionManager = PartitionManager(partition)

    def read_data(self):
        return self.segment.read()
