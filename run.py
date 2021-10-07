from bitcoin_graph import starting_info
from bitcoin_graph.btcgraph import *
from networkit import *
import argparse
import time

parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=48))
parser.add_argument('-sf', '--startfile', help=".blk start file", default="blk00000.dat")
parser.add_argument('-ef', '--endfile', help=".blk end file (excluded)", default=None)
parser.add_argument('-st', '--starttx', help="start transaction", default=None)
parser.add_argument('-et', '--endtx', help="end transaction", default=None)
parser.add_argument('-ets', '--endts', help="end timestamp of block", default=None)
parser.add_argument('-loc', '--blklocation', help=".blk file location", default="~/.bitcoin/blocks")
parser.add_argument('-f', '--format', help="networkit storage format (binary|edgelist)", default="binary")
parser.add_argument('-raw', '--rawedges', help="only build list of edges", default=None)
parser.add_argument('-wt', '--withts', help="collect list of edges with timestamps", default=None)

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
# -----------------------------------------------


# Initialize Btc graph object
# `blk_loc` for the location where the blk files are stored
# `raw Edges` to additionally save graph in EdgeList format
btc_graph = BtcGraph(dl=blk_loc, endTS=endTS, graphFormat=_format, buildRawEdges=rawEdges, withTS=withTS)

# Start building graph
btc_graph.build(startFile,endFile,startTx,endTx)
print("-----------------------------------------")