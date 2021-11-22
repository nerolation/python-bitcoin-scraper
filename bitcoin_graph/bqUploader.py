# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

# The BGUploader provides an one-stop-shop BigQuery interface for this project

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
    def __init__(self, credentials, dataset, table_id, path=None, logger=None):
        
        # put google credentials into .gcpkey folder
        self.credentials = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        self.client    = bigquery.Client()
        self.dataset   = dataset
        self.table_id  = table_id
        self.logger    = logger
        try:
            self.path  = path or "output/{}/rawedges".format(get_date())
        except:
            pass
    
    
    def upload_data(self, data=None, location="europe-west3", chsz=int(1e7), cblk=None, cvalue=None, raw=None):
        try:
            # Parsing with direct upload
            if data:
                if raw:
                    cls = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index"]
                else:
                    cls = ["ts", "txhash", "input_from", "output_to"]
                if cvalue:
                    cls.append("value")
                if cblk:
                    cls.append("blk_file_nr")
                df = pd.DataFrame(data, columns=cls)
                schema=get_table_schema(cls, cblk, cvalue, raw)
                    
                df.to_gbq(self.dataset+"."+self.table_id, if_exists="append", location=location, chunksize=chsz, table_schema=schema, progress_bar=False)
                if self.logger:
                    self.logger.log("Upload successful")
                    
            # Upload ONLY (no parsing)
            else:
                files = get_csv_files(self.path)
                r = len(files)

                # fresh printing output
                p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"

                # loop over raw edges files
                for i, blkfile in enumerate(files):
                    if "raw" in blkfile:
                        names = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index"]
                    else:   
                        names = ["ts","txhash", "from", "output_to"]
                    if cvalue:
                        names.append("value")
                    if cblk:
                        names.append("blk_file_nr")
                                           
                    df = pd.read_csv(blkfile, names=names)
                    df.to_gbq(self.dataset+"."+self.table_id, if_exists="append", location=location, chunksize=chsz, progress_bar=False)
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
