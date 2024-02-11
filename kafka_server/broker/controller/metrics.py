from prometheus_client import Counter, Gauge

coordinator_write_requests = Counter(
    'number_coordinator_write_requests',
    'The number of write requests processed by coordinator'
)

coordinator_replicate_index_requests = Counter(
    'number_coordinator_replicate_index_requests',
    'The number of replicate/index requests processed by coordinator'
)

coordinator_replicate_data_requests = Counter(
    'number_coordinator_replicate_data_requests',
    'The number of replicate/data requests processed by coordinator'
)

coordinator_subscribe_requests = Counter(
    'number_coordinator_subscribe_requests',
    'The number of subscribe requests processed by coordinator'
)
