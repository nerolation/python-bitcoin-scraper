from datetime import datetime
from google.cloud import bigquery
import os
import sys
import time
import pandas as pd
import pandas_gbq

from bitcoin_graph.helpers import _print, get_csv_files, get_date, get_table_schema

#
# Big Query Uploader
class BQUploader():
    
    # credentials: path to google credentials file, default: ./.gcpkey/
    # path: google big query path, default:output/<date>/rawedges
    # table id: google big query table id, default: btc
    # dataset: specific dataset within table, default: bitcoin_transaction
    def __init__(self, credentials, table_id, dataset, path=None, logger=None):
        
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
    
    
    def upload_data(self, data=None, location="europe-west3", chsz=int(1e7), cblk=None):
        try:
            if data:
                if len(data[0]) == 3:
                    cls = ["ts", "input_from", "output_to"]
                elif len(data[0]) == 6:
                    cls = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index"]
                elif len(data[0]) >= 7:
                    cls = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index", "value"]
                    
                if cblk:
                    cls.append("blk_file_nr")
                df = pd.DataFrame(data, columns=cls)
                schema=get_table_schema(cls, cblk)
                    
                df.to_gbq(self.table_id+"."+self.dataset, if_exists="append", location=location, chunksize=chsz, table_schema=schema)
                if self.logger:
                    self.logger.log("Upload successful")
            else:
                files = get_csv_files(self.path)
                r = len(files)

                # fresh printing output
                p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"

                # loop over raw edges files
                for i, blkfile in enumerate(files):
                    if "raw" in blkfile:
                        cols = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index"]
                        if cblk:
                            cols.append("blk_file_nr")
                        df = pd.read_csv(blkfile, names=cols)
                    else:   
                        df = pd.read_csv(blkfile, names=["ts", "from", "output_to"])
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
                return self.upload_data(data, cblk)
            if answer == "n":
                return "stop"
            return None
        
        # Catch "Table already exists" error
        # This error must not appear in theory since "if_exists" is set to "append"
        # however, sometimes it still appears. Then, just try again...
        except pandas_gbq.gbq.TableCreationError:
            return self.upload_data(data, cblk)
        
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
            return self.upload_data(data, cblk)
