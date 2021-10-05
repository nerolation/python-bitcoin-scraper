# Bitcoin-Graph 

import os, sys
import time
from termcolor import colored

__version__ = "0.1.0"
__cwd__     = os.path.abspath(os.getcwd())

def starting(args):
    print(f"Starting btc graph version {__version__} with the following arguments:")
    print("{:<18}{:<13}".format("Current wd:", __cwd__))
    for k, v in zip(args.keys(), args.values()):
        if v == True:
            v = colored("activated", "green")
        else:
            v = colored("deactivated", "red")
        print("{:<18}{:<13}".format(k+":", str(v)))
    r = 40
    p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"
    print("Initializing...")
    for i in range(r):
        sys.stdout.write(p(i+1))
        time.sleep(0.01)
    print("\n")
          
    