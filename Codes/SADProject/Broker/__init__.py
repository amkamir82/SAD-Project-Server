from Codes.SADProject.Broker.read.read_crun import *

schedule_read()

while True:
    schedule.run_pending()
    time.sleep(1)
