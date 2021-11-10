# bitcoin-graph 

**in progress**


**Bitcoin-Python-Graph** represents a **command line tool** to parse transactions directly from raw blk files.

---

The output files if the ```--raw``` flag is active are in csv format (or directly uploaded to Google BigQuery) and have the following columns:

- ts (timestamp)
- txhash (transaction hash)
- input_txhash (tx hash of input)
- vout (input vout)
- output_to (`to` address)
- output_index
- (optional) value (transfered value in satoshis)
- (optional) blk file (blk file number)


**It is recommended to use the ** ```--raw``` flag in order to save RAM since not mapping of txhash => index => address hash to be maintained!**


Alternatively the Utxos are directly mapped (input_hash == txhash so that output_index == vout), resulting in a csv with a `from` column instead of the input_hash, vout and the output_index



---


## Features
- Directly map Utxos to Input-hashes+Vout (ram intense)
- BigQuery integration
- Custom Start
-   Start at specific transaction or blk file
- Custom End
-   End execution at specefic transaction or blk file
- Optionally parse values and blk-file numbers
- Dummy address for coinbase transaction
- Integrated logging

## Installing
- in work

### Using pip
- in work


### Using source

Requirements : python-bitcoinlib, 
```
pip install -r requirements.txt
```

Install dependencies contained in `requirements.txt`:
```
pip install -r requirements.txt
```

Then, just run
```
python setup.py install
```

## Developing

First, setup a virtualenv and install dependencies:

```
virtualenv -p python3 .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Examples


### Unordered Blocks



