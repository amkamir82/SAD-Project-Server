import json
import os


def get_primary_partition() -> str:
    return os.getenv('PRIMARY_PARTITION')


def get_replica_url():
    url = os.getenv('REPLICA_URL')
    if url is None or len(url) <= 1:
        return None
    return url


def get_master_coordinator() -> str:
    return os.getenv('MASTER_COORDINATOR_URL')


def get_replica_coordinator() -> str:
    return os.getenv('REPLICA_COORDINATOR_URL')


def get_partition_count() -> int:
    brokers_file_path = os.path.join(os.getcwd(), 'data', 'subscriptions', 'brokers.json')
    if os.path.exists(brokers_file_path):
        with open(brokers_file_path, 'r', encoding='utf8') as file:
            brokers = json.load(file)
        return len(brokers)
    return 3


def is_replica_mirror_down() -> bool:
    return os.getenv('REPLICA_MIRROR_DOWN') is not None


def get_replica_mirror_down_partition() -> str:
    return os.getenv('REPLICA_MIRROR_DOWN')
