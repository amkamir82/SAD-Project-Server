import schedule

import sys, os
BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from file.read import Read


def read_sample_data():
    partition = "3"

    read_instance = Read(partition, 'http://localhost:5001')

    print(read_instance.read_data())
    # TODO: send to subscriber


def schedule_read():
    job = schedule.every(5).seconds.do(read_sample_data)
