import threading
import time
import schedule

from Codes.SADProject.Broker.read.read_crun import schedule_read

from Codes.SADProject.Broker.controller.coordinator import heartbeat


def schedule_read_thread():
    schedule_read()
    while True:
        schedule.run_pending()
        time.sleep(1)


def send_heartbeat():
    heartbeat()


read_thread = threading.Thread(target=schedule_read_thread)
read_thread.daemon = True
read_thread.start()

heartbeat_thread = threading.Thread(target=send_heartbeat)
heartbeat_thread.daemon = True
heartbeat_thread.start()
