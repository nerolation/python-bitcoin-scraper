from btcgraph import *
from networkit import *


# Static variables
startFile = None
endFile   = None
startTx   = None
endTx     = None
endTS     = None
blk_loc   = '/mnt/NewHDD/.bitcoin/blocks'
rawEdges  = True
printing  = True
# -----------------------------------------------



print("printing: {}".format(str(printing)))


btc_graph = BtcGraph(dl=blk_loc, buildRaw=rawEdges)
btc_graph.build(startFile,endFile,startTx,endTx)
print("Execution finished")