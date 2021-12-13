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
from google.cloud import bigquery, storage
import re
import os
import sys
import time
import pandas as pd
import pandas_gbq

from bitcoin_graph.helpers import _print, get_csv_files, get_date, get_table_schema
#
# Big Query Uploader
class Uploader():
    
    # credentials: path to google credentials file, default: ./.gcpkey/
    # path: google big query path, default:output/<date>/rawedges
    # table id: google big query table id, default: btc
    # dataset: specific dataset within table, default: bitcoin_transaction
    def __init__(self, credentials, project, dataset, table_id, path=None, logger=None, bucket=None):
        
        # put google credentials into .gcpkey folder
        self.credentials = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        self.client          = bigquery.Client()
        self.storage_client  = storage.Client()
        self.project         = project
        self.dataset         = dataset
        self.table_id        = table_id
        self.logger          = logger
        self.bucketname      = bucket
        try:
            self.path  = path or "output/{}/rawedges".format(get_date())
        except:
            pass
        
        existing_buckets = []
        for _bucket in self.storage_client.list_buckets():
            existing_buckets.append(_bucket.name)
        if self.bucketname not in existing_buckets:
            bucket = self.storage_client.create_bucket(self.bucketname,location="EUROPE-WEST3")
            print("Bucket created")
        
    def get_columnnames(self, raw, cvalue, cblk):
        if raw:
            cls = ["ts", "txhash", "input_txhash", "vout", "output_to", "output_index"]
        else:
            cls = ["ts", "txhash", "input_from", "output_to"]
        if cvalue:
            cls.append("value")
        if cblk:
            cls.append("blk_file_nr")
        return cls
    
    def upload_parquet_data(self, rE, blkfilenr, cblk,cvalue, raw, filechunksize = 0):
        
        cls = self.get_columnnames(raw,cvalue,cblk)
    
        df = pd.DataFrame(rE, columns=cls)
        df["vout"] = df["vout"].astype('int') 

        df.to_parquet("temp/blks_{}.parquet".format(blkfilenr))
        self.logger.log("Saved temp/blks_{}.parquet".format(blkfilenr))
        
        current_file_list = os.listdir("temp")
        
        if len(current_file_list) > filechunksize:
            for file in current_file_list:
                file = "temp/"+file
                filenr = re.search("([0-9]+)",file).group()
                bucket = self.storage_client.bucket(self.bucketname)
                blob = bucket.blob("blks_{}.parquet".format(filenr))
                blob.upload_from_filename(file, timeout=600)
                job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
                uri = "gs://{}/blks_{}.parquet".format(self.bucketname,filenr)

                load_job = self.client.load_table_from_uri(
                    uri, "{}.{}.{}".format(self.project,self.dataset,self.table_id), job_config=job_config
                )  # Make an API request.

                load_job.result()  # Waits for the job to complete
                self.logger.log("Uploaded blk file {}".format(file))
                os.remove(file)    # Delete files
        else:
            pass
        return True
        
    
    def upload_data(self, data=None, location="europe-west3", chsz=int(1e7), cblk=None, cvalue=None, raw=None):
        try:
            # Parsing with direct upload
            if data:
                cls = self.get_columnnames(raw,cvalue,cblk)
                df = pd.DataFrame(data, columns=cls)
                schema=get_table_schema(cls, cblk, cvalue, raw)
                    
                df.to_gbq(self.dataset+"."+self.table_id, if_exists="append", location=location, chunksize=chsz, table_schema=schema, progress_bar=False)
                if self.logger:
                    self.logger.log("Upload successful")
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
