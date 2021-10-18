# Console info for Btc Graph and BQ Uploader

import os, sys
import time
from termcolor import colored

__version__ = "0.1.0"
__cwd__     = os.path.abspath(os.getcwd())

# print current configuration into console
def starting_info(args):
    
    # if google big query upload
    if args["googlebigquery"]:
        for k, v in zip(args.keys(), args.values()):
            if k not in ["googlebigquery"]:
                v = colored("deactivated", "red")
            else:
                v = colored("activated", "green")
            print("{:<18}{:<13}".format(k+":", str(v)))
    
    # if building graph
    else:
        print(f"Starting btc graph version {__version__} with the following arguments:")
        if str(args["rawedges"]) not in ["False", "None", "0"]:
            args["format"] = colored("deactivated", "red")
        if str(args["withts"]) not in ["False", "None", "0"] and str(args["rawedges"]) in ["False", "None", "0"]:
            print(colored("`Withts` argument has no affect because collecting raw Edges is deactivated", "red"))
            args["withts"] = colored("deactivated", "red")
            time.sleep(2)
        if str(args["directupload"]) in ["False", "None", "0"]:
            args["credentials"] = colored("deactivated", "red")
            args["tableid"] = colored("deactivated", "red")
            args["dataset"] = colored("deactivated", "red")
        
        # Custom changes
        if args["lowmemory"] not in ["False", "None", "0"]:
            print(colored("`Low-memory mode` is activated - Inputs will represent a Tx hash and the Vout", "green"))
        print("{:<18}{:<13}".format("current wd:", __cwd__))
        for k, v in zip(args.keys(), args.values()):
            if (v and k not in ["startfile","blklocation","format","rawedges","credentials","tableid","dataset"]):
                v = colored("activated", "green")
            elif k not in ["startfile","blklocation","format", "rawedges","credentials","tableid","dataset"]:
                v = colored("deactivated", "red")
            elif str(v) == "None":
                v = colored("deactivated", "red")
            elif str(v) in ["True", "1"]:
                v = colored("activated", "green")
                
            print("{:<18}{:<13}".format(k+":", str(v)))

    for i in range(2):
        for i in ["|", "/", "-", "\\"]:
            sys.stdout.write("\rInitializing... "+i)
            time.sleep(0.3)
    sys.stdout.write("\rInitializing...   ")
    print("\n")
          
    
