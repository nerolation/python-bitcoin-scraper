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
from google.api_core.exceptions import BadRequest
import re
import os
import sys
import time
import pandas as pd
import pandas_gbq

from bitcoin_graph.helpers import _print, get_date, get_table_schema
#
# Big Query Uploader
class Uploader():
    
    # credentials: path to google credentials file, default: ./.gcpkey/
    # path: google big query path, default:output/<date>/rawedges
    # table id: google big query table id, default: btc
    # dataset: specific dataset within table, default: bitcoin_transaction
    def __init__(self, credentials, project, dataset, table_id, path=None, 
                 logger=None, bucket=None, pthreshold=None, multi_p=False):
        
        # put google credentials into .gcpkey folder
        self.credentials = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials or input("No Google API credendials file provided."\
        									   "Please specify path now:\n")
        self.client          = bigquery.Client()
        self.storage_client  = storage.Client()
        self.project         = project
        self.dataset         = dataset
        self.table_id        = table_id
        self.logger          = logger
        self.threshold       = pthreshold
        self.bucketname      = bucket
        self.multi_p         = multi_p
        try:
            self.path  = path or "output/{}/rawedges".format(get_date())
        except:
            pass
        
        if bucket:
            existing_buckets = []
            for _bucket in self.storage_client.list_buckets():
                existing_buckets.append(_bucket.name)
            if self.bucketname not in existing_buckets:
                bucket = self.storage_client.create_bucket(self.bucketname,location="EUROPE-WEST3")
                print("Bucket created")
        
    def get_columnnames(self, cvalue, cblk):
        
        # Default column names
        cls = ["ts", "tx_id", "input_tx_id", "vout", "output_to", "output_index"]
        if cvalue:
            cls.append("value")
        if cblk:
            cls.append("blk_file_nr")
        return cls
    
    def end_not_yet_reached(self):
        ''' Returns `True` if parsing has ended and no more files to upload '''
        if len(os.listdir(".temp")) > 1:
            return True
        else:
            with open("./.temp/end_multiprocessing.txt", "r") as file:
                return not eval(file.read())
    
    def upload_parquet_data(self):
        while(self.end_not_yet_reached()):
            current_file_list = os.listdir(".temp")
            if len(current_file_list) > 1:
                for file in current_file_list:
                    file = ".temp/" + file
                    if file.endswith("txt"):
                        continue
                    if datetime.now().timestamp()-os.path.getmtime(file) < 20:
                        continue
                    
                    filenr = re.search("([0-9]+)",file).group()
                    bucket = self.storage_client.bucket(self.bucketname)
                    blob = bucket.blob("blk_{}.parquet".format(filenr))
                    blob.upload_from_filename(file, timeout=600)
                    
                    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
                    uri = "gs://{}/blk_{}.parquet".format(self.bucketname,filenr)
                    
                    worked = False
                    while not worked:
                        try:
                            load_job = self.client.load_table_from_uri(
                                uri, "{}.{}.{}".format(self.project,self.dataset,self.table_id), job_config=job_config
                            )  # Make an API request

                            load_job.result()  # Waits for the job to complete
                            worked = True
                            
                         except BadRequest:
                            time.sleep(10)
                            
                    os.remove(file)    # Delete file
                    blob.delete()
                    _print(f"{file} uploaded", end="\r")
                    self.logger.log("Uploaded blk file {}".format(file))

            else:
                time.sleep(5)
        return True

    def handle_parquet_data(self, rE, blkfilenr, cblk, cvalue):
        
        cls = self.get_columnnames(cvalue,cblk)
    
        df = pd.DataFrame(rE, columns=cls)
        df["vout"] = df["vout"].astype('int') 
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(lambda x:re.sub('[^A-Za-z0-9]+','', str(x)))

        df.to_parquet(".temp/blk_{}.parquet".format(blkfilenr))
        self.logger.log("Saved .temp/blk_{}.parquet".format(blkfilenr))
        if not self.multi_p:
            current_file_list = os.listdir(".temp")

            if len(current_file_list) > self.threshold:
                for file in current_file_list:
                    file = ".temp/" + file
                    filenr = re.search("([0-9]+)",file).group()
                    bucket = self.storage_client.bucket(self.bucketname)
                    blob = bucket.blob("blk_{}.parquet".format(filenr))
                    blob.upload_from_filename(file, timeout=600)
                    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
                    uri = "gs://{}/blk_{}.parquet".format(self.bucketname,filenr)

                    load_job = self.client.load_table_from_uri(
                        uri, "{}.{}.{}".format(self.project,self.dataset,self.table_id), job_config=job_config
                    )  # Make an API request
                    
                    load_job.result()  # Waits for the job to complete
                    _print(f"{file} uploaded", end="  \r")
                    self.logger.log("Uploaded blk file {}".format(file))
                    os.remove(file)    # Delete files
        else:
            pass
        
        return True
        
    
    def upload_data(self, data=None, location="europe-west3", chsz=int(1e7), cblk=None, cvalue=None):
        try:
            # Parsing with direct upload
            cls = self.get_columnnames(cvalue,cblk)
            df = pd.DataFrame(data, columns=cls)
            schema=get_table_schema(cls, cblk, cvalue)
            cloud_path = self.dataset+"."+self.table_id
            df.to_gbq(cloud_path, 
                      if_exists="append", 
                      location=location, 
                      chunksize=chsz, 
                      table_schema=schema, 
                      progress_bar=False)
            if self.logger:
                self.logger.log("Upload successful")
            return True
        
        # Crtl + C to skip upload
        except KeyboardInterrupt:
            self.logger.log("Upload skipped")
            _print("\nUpload skipped ...")
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
            _print("\nSomething failed")
            _print(e)
            _print("Do you want to wait for some seconds to try again? "\
                   "(please input seconds to wait)")
            wait = int(input())
            _print(f"Waiting for {wait} seconds")
            time.sleep(wait)
            _print("Trying again...")
            return self.upload_data(data, cblk)
