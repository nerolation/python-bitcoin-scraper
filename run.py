from bitcoin_graph import starting_info
from bitcoin_graph.btcgraph import *
from bitcoin_graph.bquploader import *
from networkit import *
import argparse
import time

parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80))
parser.add_argument('-sf', '--startfile', help=".blk start file - default: blk00000.dat", default="blk00000.dat")
parser.add_argument('-ef', '--endfile', help=".blk end file (excluded) - default: None", default=None)
parser.add_argument('-st', '--starttx', help="start transaction - default: None", default=None)
parser.add_argument('-et', '--endtx', help="end transaction - default: None", default=None)
parser.add_argument('-ets', '--endts', help="end timestamp of block - default: None", default=None)
parser.add_argument('-loc', '--blklocation', help=".blk file location - default: ~/.bitcoin/blocks", default="~/.bitcoin/blocks")
parser.add_argument('-f', '--format', help="networkit storage format (binary|edgelist) - default: binary", default="binary")
parser.add_argument('-raw', '--rawedges', help="path to store raw edges - default: No", default=None)
parser.add_argument('-wts', '--withts', help="collect list of edges with timestamps - default: No", default=None)
parser.add_argument('-gbq', '--googlebigquery', help="upload edges to google bigquery - default: False", default=None)
parser.add_argument('-up', '--directupload', help="upload edges directly(!) to google bigquery - default: False", default=None)

# Handle parameters
_args = parser.parse_args()

# Print some env info
starting_info(vars(_args))

# Static variables
startFile = _args.startfile
endFile   = _args.endfile
startTx   = _args.starttx
endTx     = _args.endtx
endTS     = _args.endts
blk_loc   = _args.blklocation
_format   = _args.format
rawEdges  = _args.rawedges
withTS    = _args.withts
gbq       = _args.googlebigquery
upload    = _args.directupload
# -----------------------------------------------

if not gbq:
    # Initialize btc graph object
    # `blk_loc` for the location where the blk files are stored
    # `raw Edges` to additionally save graph in edgeList format
    btc_graph = BtcGraph(dl=blk_loc, endTS=endTS, graphFormat=_format, buildRawEdges=rawEdges, withTS=withTS, upload=upload)

    # Start building graph
    btc_graph.build(startFile,endFile,startTx,endTx)
    
else:
    # Initialize Big Query Uploader
    bq = bqUpLoader()
    
    # Upload raw edges csv files to google cloud/big-query
    bq.upload_data()
    
print("-----------------------------------------")