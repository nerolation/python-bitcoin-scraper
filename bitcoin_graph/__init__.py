# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

# Console info after initiating the btc parser or uploader to show current settings

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
        if str(args["withts"]) in ["False", "None", "0"]:
            args["withts"] = 0
        if str(args["withvalue"]) in ["False", "None", "0"]:
            args["withvalue"] = 0
        if str(args["directupload"]) in ["False", "None", "0"]:
            args["credentials"] = colored("deactivated", "red")
            args["tableid"] = colored("deactivated", "red")
            args["dataset"] = colored("deactivated", "red")
            args["directupload"] = 0
        # Custom changes
        if args["raw"] in ["False", "None", "0"]:
            args["raw"] = 0
        else:
            print(colored("Raw parsing is activated - Inputs will represent a Tx hash and the Vout", "green"))
        print("{:<18}{:<13}".format("current wd:", __cwd__))
        for k, v in zip(args.keys(), args.values()):
            if (v and k not in ["startfile","blklocation","format","localpath","credentials","tableid","dataset"]):
                v = colored("activated", "green")
            elif k not in ["startfile","blklocation","format", "localpath","credentials","tableid","dataset"]:
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
          
    
