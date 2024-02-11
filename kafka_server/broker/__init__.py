import threading
import time
import sys
import os

BROKER_PROJECT_PATH = os.getenv("BROKER_PROJECT_PATH", "/app/")
sys.path.append(os.path.abspath(BROKER_PROJECT_PATH))

from read.read_cronjob import *

def second_thread_function():
    schedule_read()
    while True:
        schedule.run_pending()
        time.sleep(1)


second_thread = threading.Thread(target=second_thread_function)
second_thread.daemon = True
second_thread.start()