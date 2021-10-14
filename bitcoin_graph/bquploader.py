from datetime import datetime
from google.cloud import bigquery
import os
import sys
import time
import pandas as pd
import pandas_gbq

def get_csv_files(path):
        files = os.listdir(path)
        files = [f for f in files if f.startswith("raw") and f.endswith(".csv")]
        files = map(lambda x: os.path.join(path, x), files)
        return sorted(files)
    
def get_date(folder="./output"):
    content = [fn for fn in os.listdir(folder)]
    dates = [datetime.strptime(fn, "%Y%m%d_%H%M%S") for fn in content]
    return dates[dates.index(max(dates))].strftime("%Y%m%d_%H%M%S")

#
# Big Query Uploader
class bqUpLoader():
    
    # credentials: path to google credentials file, default: ./.gcpkey/
    # path: google big query path, default:output/<date>/rawedges
    # table id: google big query table id, default: btc
    # dataset: specific dataset within table, default: bitcoin_transaction
    def __init__(self, credentials=None, path=None, table_id=None, dataset=None):
        
        # put google credentials into .gcpkey folder
        self.credentials = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        self.client    = bigquery.Client()
        self.table_id  = table_id
        self.dataset   = dataset 
        
        try:
            self.path  = path or "output/{}/rawedges".format(get_date())
        except:
            pass
    
    
    def upload_data(self, data=None):
        try:
            if data:
                df = pd.DataFrame(data, columns=["ts", "from", "to"])
                df.to_gbq(self.table_id+"."+self.dataset, if_exists="append", chunksize=int(1e8))
            else:
                files = get_csv_files(self.path)
                r = len(files)

                # fresh printing output
                p = lambda x: "\r[" + "#" * x + (r - x) * " " + "]"

                # loop over raw edges files
                for i, blkfile in enumerate(files):
                    df = pd.read_csv(blkfile, names=["ts", "from", "to"])
                    df.to_gbq(self.table_id+"."+self.dataset, if_exists="append", chunksize=int(1e8))
                    time.sleep(2)
                    sys.stdout.write("\r\r{:<18} successfully uploaded   \n".format(blkfile.split("/")[-1]))            
                    sys.stdout.write(p(i+1))
                    time.sleep(0.1)
                print()
        except KeyboardInterrupt:
            print("Upload skiped ...")
           
