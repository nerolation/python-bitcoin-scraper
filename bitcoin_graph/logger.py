import os
from datetime import datetime

# Logger object
class BlkLogger:
    def __init__(self):
        if not os.path.isdir('logs/'):
            _print("Creating logs folder...")
            os.makedirs('logs')

    def log(self, s):
        ts = datetime.now().strftime("%Y-%m-%d  |  %H:%M:%S ")
        with open("logs/logs.txt", "a") as logfile:
            logfile.write(ts + s + "\n")