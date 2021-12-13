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

__version__ = "0.2.0"
__cwd__     = os.path.abspath(os.getcwd())

# print current configuration into console
def starting_info(args):

    print(colored(f"\nStarting python-bitcoin-graph version {__version__} with the following arguments:", attrs=['bold']))
    if str(args["collectvalue"]) in ["False", "None", "0"]:
        args["collectvalue"] = 0
    if str(args["directupload"]) in ["False", "None", "0"]:
        args["credentials"] = colored("deactivated", "red")
        args["project"] = colored("deactivated", "red")
        args["tableid"] = colored("deactivated", "red")
        args["dataset"] = colored("deactivated", "red")
        args["directupload"] = 0
    else:
        args["targetpath"] = colored("deactivated", "red")
    # Custom changes
    if args["raw"] in ["False", "None", "0"]:
        args["raw"] = 0
    else:
        print(colored("Raw parsing is activated - Inputs will represent a Tx hash and the Vout\n", "green",attrs=['bold']))
    if not str(args["parquet"]):
        args["bucket"] = 0
    print("{:<25}{:<13}".format("current wd:", __cwd__))
    non_bools = ["startfile","blklocation","format","targetpath","credentials",
                 "project","tableid","dataset","bucket","uploadthreshold"]
    for k, v in zip(args.keys(), args.values()):
        if (v and k not in non_bools):
            v = colored("activated", "green")
        elif k not in non_bools:
            v = colored("deactivated", "red")
        elif str(v) == "None":
            v = colored("deactivated", "red")
        elif str(v) in ["True", "1"]:
            v = colored("activated", "green")

        print("{:<25}{:<13}".format(k+":", str(v)))
    print("\n")  
    
    if args["parquet"]:
        if not os.path.isdir('./temp'):
            os.makedirs('./temp')
        elif len(os.listdir('./temp')) > 0:
            delete = input("There are already files in the ./temp folder\nDo you want to delet them? (y/n)\n")
            if delete == "y":
                for tempfile in os.listdir('./temp'):
                    os.remove('./temp/'+tempfile) 
            print("\r\r           ")
    for i in range(2):
        for i in ["|", "/", "-", "\\"]:
            sys.stdout.write("\rInitializing... "+i)
            time.sleep(0.3)
    sys.stdout.write("\rInitializing...   \n")
    sys.stdout.flush() 
    
          
    
