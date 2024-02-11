import threading
import time

from Codes.SADProject.Broker.read.read_crun import *

def second_thread_function():
    schedule_read()
    while True:
        schedule.run_pending()
        time.sleep(1)


second_thread = threading.Thread(target=second_thread_function)
second_thread.daemon = True
second_thread.start()