import os

def get_primary_partition() -> str:
    return os.getenv('PRIMARY_PARTITION', '2')


def get_replica_url() -> str:
    return 'http://broker-3:5003'
    # return os.getenv('REPLICA_URL')


def get_master_coordinator() -> str:
    return os.getenv('MASTER_COORDINATOR_URL')


def get_replica_coordinator() -> str:
    return os.getenv('REPLICA_COORDINATOR_URL')


def get_partition_count() -> int:
    return int(os.getenv('PARTITION_COUNT', 3))
