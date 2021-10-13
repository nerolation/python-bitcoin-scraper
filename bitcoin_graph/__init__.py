# Bitcoin-Graph 

import os, sys
import time
from termcolor import colored

__version__ = "0.1.0"
__cwd__     = os.path.abspath(os.getcwd())

def starting_info(args):
    print(f"Starting btc graph version {__version__} with the following arguments:")
    if str(args["rawedges"]) not in ["False", "None", "0"]:
        args["format"] = colored("deactivated", "red")
    if str(args["withts"]) not in ["False", "None", "0"] and str(args["rawedges"]) in ["False", "None", "0"]:
        print(colored("`Withts` argument has no affect because collecting raw Edges is deactivated", "red"))
        args["withts"] = colored("deactivated", "red")
        time.sleep(2)
    print("{:<18}{:<13}".format("current wd:", __cwd__))
    for k, v in zip(args.keys(), args.values()):
        if (v and k not in ["startfile","blklocation","format"]):
            v = colored("activated", "green")
        elif k not in ["startfile","blklocation","format"]:
            v = colored("deactivated", "red")
        print("{:<18}{:<13}".format(k+":", str(v)))
    r = 40
    p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"
    print("\nInitializing...")
    for i in range(r):
        sys.stdout.write(p(i+1))
        time.sleep(0.01)
    print("\n")
          
    
