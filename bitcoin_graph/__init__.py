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

__version__ = "0.2.1"
__cwd__     = os.path.abspath(os.getcwd())

# print current configuration into console
def starting_info(args):

    print(colored(f"\nStarting python-bitcoin-graph version {__version__} "\
                  "with the following arguments:\n", attrs=['bold']))
    if str(args["collectvalue"]) in ["False", "None", "0"]:
        args["collectvalue"] = 0
    
    if not args["startfile"] or not args["endfile"]:
        files = []
        for file in os.listdir(args["blklocation"]):
            if file.endswith(".dat") and "blk" in file:
                files.append(file)
        files = sorted(files)
        if not args["startfile"]:
            args["startfile"] = files[0]
        if not args["endfile"]:
            args["endfile"]   = files[-1]
            
    
    
    if str(args["upload"]) in ["False", "None", "0"]:
        args["credentials"] = colored("deactivated", "red")
        args["project"] = colored("deactivated", "red")
        args["tableid"] = colored("deactivated", "red")
        args["dataset"] = colored("deactivated", "red")
        args["upload"] = 0
        if args["parquet"]:
            print(colored("Use parquet mode only together with the --upload flag"
                          , "red", attrs=['bold']))
            raise Exception("Set --upload flag")
            
        if args["multiprocessing"] or args["parquet"]:
            print(colored("Make sure to set the upload flag when using parquet mode\n", "red", attrs=['bold']))
            time.sleep(1)
            raise Exception("Try to set --upload flag")

    else:
        args["targetpath"] = colored("deactivated", "red")
        
    # Custom changes
    if args["parquet"]:
        print(colored("Parquet format is activated - Files will be converted to parquet "\
                      "format before syncing to the Google cloud\n", "green", attrs=['bold']))
            
    else:
        args["bucket"] = None
        args["uploadthreshold"] = None
        if args["multiprocessing"]:
            print(colored("Multiprocessing was deactivated - only supported with parquet format"
                          , "red", attrs=['bold']))
            args["multiprocessing"] = 0
            
        
    print("{:<25}{:<13}".format("current wd:", __cwd__))
    non_bools = ["startfile","endfile","blklocation","format","targetpath","credentials",
                 "project","tableid","dataset","bucket","uploadthreshold"]
    
    # Manage bool arguments
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
        if not os.path.isdir('{}/../.temp'.format(args["blklocation"])):
            os.makedirs('{}/../.temp'.format(args["blklocation"]))
        elif len(os.listdir('{}/../.temp'.format(args["blklocation"]))) > 0:
            for tempfile in os.listdir('{}/../.temp'.format(args["blklocation"])):
                os.remove('{}/../.temp/{}'.format(args["blklocation"],tempfile))
            print("\r\r           ")
    for i in range(2):
        for i in ["|", "/", "-", "\\"]:
            sys.stdout.write("\rInitializing... "+i)
            time.sleep(0.3)
    sys.stdout.write("\rInitializing...   \n")
    sys.stdout.flush() 
    
          
    
