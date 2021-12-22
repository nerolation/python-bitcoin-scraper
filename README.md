# bitcoin-graph 


**Bitcoin-Python-Graph** represents a **command line tool** to parse transactions directly from raw blk files using Python. 


---

*The output files are in **csv** or **parquet format** and can be directly synced to Google BigQuery.*

By default the following columns are collected:

- ts (timestamp)
- tx_id (transaction id)
- input_tx_id (tx id of input)
- vout (input vout)
- output_to (`to` address)
- output_index (index of output address)
- (optional) value (transfered value in satoshis)
- (optional) blk file (blk file number)


**Note: The transaction inputs are represented by a tx id and the vout!** 

## Usage

```console
$ python3 run.py -loc ./data --collectvalue --collectblk --upload --parquet --multiprocessing

optional arguments:
  -h, --help                                              show this help message and exit
  -sf STARTFILE, --startfile STARTFILE                    .blk start file (included) - default: blk00000.dat
  -ef ENDFILE, --endfile ENDFILE                          .blk end file (excluded) - default: None
  -st STARTTX, --starttx STARTTX                          start transaction id (included) - default: None
  -et ENDTX, --endtx ENDTX                                end transaction id (excluded) - default: None
  -ets ENDTS, --endts ENDTS                               end timestamp of block - default: None
  -loc BLKLOCATION, --blklocation BLKLOCATION             .blk|.csv file location - default: ~/.bitcoin/blocks
  -path TARGETPATH, --targetpath TARGETPATH               path to store raw edges locally - default: ./
  -collectvalue, --collectvalue                           collect output values - default: No
  -collectblk, --collectblk                               collect blk file numbers with every edge - default: No
  -upload, --upload                                       upload edges to google bigquery - default: False
  -parquet, --parquet                                     use parquet format - default: False
  -mp, --multiprocessing                                  use multiprocessing - default: False
  -ut UPLOADTHRESHOLD, --uploadthreshold UPLOADTHRESHOLD  uploading threshold for parquet files - default: 5
  -bucket BUCKET, --bucket BUCKET                         bucket name to store parquet files - default: btc_<timestamp>
  -c CREDENTIALS, --credentials CREDENTIALS               path to google credentials (.*json)- default: ./.gcpkey/.*json
  -project PROJECT, --project PROJECT                     google cloud project name - default: btcgraph
  -ds DATASET, --dataset DATASET                          bigquery data set name - default: btc
  -tid TABLEID, --tableid TABLEID                         bigquery table id - default: bitcoin_transactions
```
If uploading is activated, it is highly recommended to consider the integrated parquet-format conversion before uploading the data to the Google Cloud in order to reduce bandwidth usage. This can easily be done using the  `--parquet` flag. Using parquet format you can boost execution by activating multiprocessing - using the `-mp` flag - and parallely parse and upload block files.

---


## Features
- Multiprocessing
- Parquet format integration (compressed files for faster uploads)
- BigQuery integration 
- Custom Start possibilities
-   Start at specific transaction or blk file
- Custom End
-   End execution at specefic transaction or blk file
- Optionally parse values and blk-file numbers
- Dummy address for coinbase transaction
- Supports invalid and unknown scripts + OP_RETURNs
- Integrated logging

## Installing


### Using source

Install dependencies contained in `requirements.txt`:
```
pip install -r requirements.txt
```


## Examples
[![asciicast](https://asciinema.org/a/458061.svg)](https://asciinema.org/a/458061)
---

**Note: Currently it takes about 4 days to parse and upload the complete blockchain!** 




