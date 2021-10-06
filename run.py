from bitcoin_graph import starting_info
from bitcoin_graph.btcgraph import *
from networkit import *
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('-sf', '--startfile', default="blk00000.dat")
parser.add_argument('-ef', '--endfile', default=None)
parser.add_argument('-st', '--starttx', default=None)
parser.add_argument('-et', '--endtx', default=None)
parser.add_argument('-ets', '--endtimestamp', default=None)
parser.add_argument('-loc', '--blklocation', default="~/.bitcoin/blocks")
parser.add_argument('-f', '--format', default="binary")
parser.add_argument('-raw', '--rawedges', default=False)
parser.add_argument('-p', '--printing', default=True)

# Handle parameters
_args = parser.parse_args()

# Print some env info
starting_info(vars(_args))


# Static variables
startFile = _args.startfile
endFile   = _args.endfile
startTx   = _args.starttx
endTx     = _args.endtx
endTS     = _args.endtimestamp
blk_loc   = _args.blklocation
_format   = _args.format
rawEdges  = _args.rawedges
printing  = _args.printing
# -----------------------------------------------


# Initialize Btc graph object
# `blk_loc` for the location where the blk files are stored
# `raw Edges` to additionally save graph in EdgeList format
btc_graph = BtcGraph(dl=blk_loc, endTS=endTS, graphFormat=_format, buildRaw=rawEdges, printing=printing)

# Start building graph
btc_graph.build(startFile,endFile,startTx,endTx)
print("Execution finished")