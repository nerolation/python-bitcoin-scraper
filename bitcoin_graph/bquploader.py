from datetime import datetime
from google.cloud import bigquery
import os
import sys
import time
import pandas as pd
import pandas_gbq

from bitcoin_graph.helpers import _print, get_csv_files, get_date

#
# Big Query Uploader
class bqUpLoader():
    
    # credentials: path to google credentials file, default: ./.gcpkey/
    # path: google big query path, default:output/<date>/rawedges
    # table id: google big query table id, default: btc
    # dataset: specific dataset within table, default: bitcoin_transaction
    def __init__(self, credentials=None, path=None, table_id=None, dataset=None, logger=None):
        
        # put google credentials into .gcpkey folder
        self.credentials = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        self.client    = bigquery.Client()
        self.table_id  = table_id
        self.dataset   = dataset 
        self.logger    = logger
        try:
            self.path  = path or "output/{}/rawedges".format(get_date())
        except:
            pass
    
    
    def upload_data(self, data=None, location="europe-west3", chsz=None):
        try:
            if data:
                if len(data[0]) == 3:
                    df = pd.DataFrame(data, columns=["ts", "from", "to"])
                elif len(data[0]) == 6:
                    df = pd.DataFrame(data, columns=["ts", "txhash", "input_txhash", "vout", "to", "output_index"])
                df.to_gbq(self.table_id+"."+self.dataset, if_exists="append", location=location, chunksize=chsz)
                self.logger.log("Upload successful")
            else:
                files = get_csv_files(self.path)
                r = len(files)

                # fresh printing output
                p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"

                # loop over raw edges files
                for i, blkfile in enumerate(files):
                    if "lm" in blkfile:
                        df = pd.read_csv(blkfile, names=["ts", "txhash", "input_txhash", "vout", "to", "output_index"])
                    else:   
                        df = pd.read_csv(blkfile, names=["ts", "from", "to"])
                    df.to_gbq(self.table_id+"."+self.dataset, if_exists="append", location=location, chunksize=chsz)
                    sys.stdout.write("\r\r{:<18} successfully uploaded   \n".format(blkfile.split("/")[-1]))            
                    sys.stdout.write(p(i+1))
                print()
            return True
        
        # Crtl + C to skip upload
        except KeyboardInterrupt:
            self.logger.log("Upload skipped")
            _print("Upload skipped ...")
            answer = None
            while answer not in ["n", "y", "re-upload"]:
                _print("Do you want to continue? (y/n/re-upload)")
                answer = input()
            if answer == "re-upload":
                return self.upload_data(data)
            if answer == "n":
                return "stop"
            return None
        
        # Catch other errors and let user set time to wait
        except Exception as e:
            self.logger.log("Something failed")
            _print("Something failed")
            _print(e)
            _print("Do you want to wait for some seconds to try again? (please input seconds to wait)")
            wait = int(input())
            _print(f"Waiting for {wait} seconds")
            time.sleep(wait)
            _print("Trying again...")
            return self.upload_data(data)
