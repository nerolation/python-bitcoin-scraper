# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

# Simple logger module with little extra functionality

import os
from datetime import datetime

# Logger object
class BlkLogger:
    def __init__(self):
        if not os.path.isdir('logs/'):
            _print("Creating logs folder...")
            os.makedirs('logs')

    def log(self, s):
        ts = datetime.now().strftime("%Y-%m-%d  |  %H:%M:%S   ")
        with open("logs/logs.txt", "a") as logfile:
            logfile.write(ts + s + "\n")