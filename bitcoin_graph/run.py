from __init__ import __version__
from btcgraph import *
from networkit import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-sf', '--startfile', default=None)
parser.add_argument('-ef', '--endfile', default=None)
parser.add_argument('-st', '--starttx', default=None)
parser.add_argument('-et', '--endtx', default=None)
parser.add_argument('-ets', '--endtimestamp', default=None)
parser.add_argument('-loc', '--blklocation', default="~/.bitcoin/blocks")
parser.add_argument('-raw', '--rawedges', default=True)
parser.add_argument('-p', '--printing', default=True)

_args = parser.parse_args()
args  = vars(_args)


print(f"Starting btc graph version {__version__} with the following arguments:")
for k, v in zip(args.keys(), args.values()):
    print("{:<13}{:>13}".format(k+":", str(v)))
print()

# Static variables
startFile = _args.startfile
endFile   = _args.endfile
startTx   = _args.starttx
endTx     = _args.endtx
endTS     = _args.endtimestamp
blk_loc   = _args.blklocation
rawEdges  = _args.rawedges
printing  = _args.printing
# -----------------------------------------------



btc_graph = BtcGraph(dl=blk_loc, buildRaw=rawEdges)
btc_graph.build(startFile,endFile,startTx,endTx)
print("Execution finished")