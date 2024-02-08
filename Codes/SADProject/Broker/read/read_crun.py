import schedule
from Codes.SADProject.Broker.file.read import Read


def read_sample_data():
    partition = "3"

    read_instance = Read(partition, 'http://localhost:5001')

    print(read_instance.read_data())
    # todo: send to subscriber


def schedule_read():
    job = schedule.every(5).seconds.do(read_sample_data)
