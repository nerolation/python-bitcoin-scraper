import os
import sys
import time

import pandas as pd
import matplotlib.pyplot as plt

from termcolor import colored
from datetime import datetime
from networkit import *

from bitcoin_graph.blockchain_parser.blockchain import Blockchain
from bitcoin_graph.input_heuristic import InputHeuristic
from bitcoin_graph.bquploader import bqUpLoader, _print
from bitcoin_graph.logger import BlkLogger
from bitcoin_graph.helpers import *


# ----------
## BtcGraph
#

class BtcGraph:
    
    def __init__(self, G=None, V=None, Utxos=None, MAP_V=None, Raw_Edges=None, Meta=None,
                 dl='~/.bitcoin/blocks', buildRawEdges=False, withTS=None, endTS=None, 
                 graphFormat="binary", upload=False, credentials=None, table_id=None, dataset=None,
                 lowMemory=False, clusterInputs=False, iC=None
                ):
        self.endTS        = endTS               # Timestamp of last block
        self.dl           = dl                  # Data location where blk files are stored
        self.G            = G or graph.Graph(n=0,directed=True) # NetworKit Graph
        self.V            = V or set()          # Set of vertices/nodes of Bitcoin Addresses
        self.Utxos        = Utxos or {}         # Mapping of Tx Hash => Utxo Indices => Output Addresses
        self.MAP_V        = MAP_V or {}         # Mapping of Bitcoin Addresses => Indices (NetworKit Integers)
        self.MAP_V_r      = MAP_V_r(self.MAP_V) # Reversed MAP_V mapping
        self.buildRawEdges= buildRawEdges       # Path to store a list of raw edges 
        self.withTS       = withTS              # Bool value to decide whether timestamps are collected
        self.Raw_Edges    = Raw_Edges or []     # Raw Edges
        self.communities  = None                # Communities placeholder
        self.logger       = BlkLogger()         # Logging module
        self.graphFormat  = graphFormat         # Graph format 
        self.upload       = upload              # Bool to directly upload to GCP
        self.lowMemory    = lowMemory           # Bool to activate low-memory-consumption mode
        if self.upload:
            self.creds    = credentials         # Path to google credentials json
            self.table_id = table_id            # GBQ table id
            self.dataset  = dataset             # GBQ data set name
            self.uploader = bqUpLoader(credentials=self.creds,
                                       table_id=self.table_id,
                                       dataset=self.dataset,
                                       logger=self.logger) # BigQuery uploader
            
        
        # Meta data
        self.currTxHash   = Meta[3] if Meta else None # Last Tx Hash processed
        self.currBlHash   = Meta[2] if Meta else None # Last Block Hash processed
        self.lastBlTs     = Meta[1] if Meta else None # Last Timestamp object of block processed
        self.lastBlTs_s   = int(self.lastBlTs.timestamp()) if Meta else None # Last Timestamp
        self.creationTime = Meta[0] if Meta else datetime.now() # Creation time of `this`

        # Heuristic 1
        self.clusterInputs= clusterInputs          # Bool value to cluster combined Inputs to one node
        self.inputCluster = iC or InputHeuristic() # InputHeuristic obj. to cluster based on Inputs

        # Placehoder for undirected copy of G
        if self.G.isDirected():
            self._G = None
        
        # Add communities to object (if Graph exists already)
        try:
            self.communities = community.detectCommunities(_G, algo=community.PLM(_G, True))
        except:
            pass
        
        # Timestamp to datetime object
        if self.endTS:
            self.endTS=datetime.fromtimestamp(int(self.endTS))
        
        # Load existing Utxos mapping if path was specified
        if self.Utxos:
            self.Utxos = load_Utxos(self.Utxos)
        
        _print("New BtcGraph initialized")
        time.sleep(1)
    
    def _addEdge(self, u, v):
        self.G.addEdge(u=u,v=v,addMissing=False)

    def _addNode(self, n):
        ix = self.G.addNode()
        self._update_mapping(n, ix)
        self.V.add(n)
        return ix
    
    def _buildEdge(self, u, v):
        if self.buildRawEdges:
            for _u in set(u):
                # Collect Vout in low-memory mode
                if self.lowMemory:
                    for _index, _v in enumerate(v):
                        if self.withTS:
                            self.Raw_Edges.append((self.lastBlTs_s,self.currTxHash,_u, _v, _index))
                        else:
                            self.Raw_Edges.append((self.currTxHash,_u, _v, _index))
                else:
                    for _v in v:
                        if self.withTS:
                            self.Raw_Edges.append((self.lastBlTs_s,_u, _v))
                        else:
                            self.Raw_Edges.append((_u, _v))
            return
        
        # Directly build graph
        else:
            # Inputs
            inputs  = [self._addNode(_u) if _u not in self.V else self.MAP_V[_u] for _u in u]

            # Outputs
            outputs = [self._addNode(_v) if _v not in self.V else self.MAP_V[_v] for _v in v]

            for i in inputs:
                for o in outputs:
                    self._addEdge(i,o)
    
            
    def _update_mapping(self, node, idx):
        self.MAP_V[node] = idx
        
    # Build Graph
    # Arguments: start-file sF, end-file eF, start-Tx sT, end-Tx eT
    def build(self, sF, eF, sT, eT): 
        global now
        _print("Start building...")
        try:
            # Instantiate the Blockchain by giving the path to the directory
            # containing the .blk files created by bitcoind
            now = datetime.now().strftime("%Y%m%d_%H%M%S")
            blockchain = Blockchain(os.path.expanduser(self.dl))
            
            # Set start to True if no Start transaction is provided
            start = True if sT == None else False
            
            # Receive list of .blk files
            blk_files = blockchain.get_blk_files(sF, eF)
            
            # l = number of .blk files
            # t0 = time iteration beginns
            # loop_duration = list of delta times of iterations
            l, t0, loop_duration = len(blk_files), None, []
            
            # Loop through all .blk files
            for blk_file in blk_files:
                
                # Ensure to start with an empty array
                assert(len(self.Raw_Edges) == 0)
                
                # Get integer of .blk filename (blk00001 => 1)
                fn = file_number(blk_file)
                
                # Log progress
                _print(colored(f"Block File # {fn}/{l}", "green"))
                self.logger.log(f"Block File # {fn}/{l}")

                _print(f"Processing {blk_file}")
                for block in blockchain.get_unordered_blocks(blk_file):
                    
                    # Keep track of processed blocks
                    self.currBlHash = block.hash

                    # Skip blocks younger than specified `end timestamp`
                    self.lastBlTs   = block.header.timestamp
                    self.lastBlTs_s = int(self.lastBlTs.timestamp())
                    if self.endTS:                     
                        if self.lastBlTs > self.endTS:
                            continue
                    
                    for tx in block.transactions:
                        
                        # ---
                        # Custom Start or End
                        # Set `last-processed hash`
                        self.currTxHash = tx.hash
                        
                         # Try to start if start-transaction sT is reached
                        if start == False:
                            start = True if (sT == None or sT == self.currTxHash) else False

                        # Try to stop execution afer last-transaction-hash is reached
                        if eT != None and start == True:
                            start = False if eT == self.currTxHash else True
                            if start == False:
                                _print("End Tx reached")
                                _print("Execution terminated")
                                sys.exit(1)
                        # ---
                        
                        if start:
                            # Skip collecting outputs in low-memory mode
                            if not self.lowMemory:
                                # Handle Outputs
                                outs={}
                                for o_index, output in enumerate(tx.outputs):
                                    outs[o_index] = [address.address for address in output.addresses]

                                self.Utxos[tx.hash] = outs

                            # Handle Inputs
                            Vins = []
                            for inp in tx.inputs:

                                # Coinbase Txs
                                if inp.transaction_hash == "0" * 64:
                                    # Build egde from ZERO to all Transaction output addresses
                                    Vins.append("00")
                                    
                                # If possible to connect inputs -> prev. outputs
                                elif inp.transaction_hash in self.Utxos:

                                    # Inputs (Vin)
                                    Vout = inp.transaction_index
                                    Vin = self.Utxos[inp.transaction_hash][Vout]
                                 
                                    Vins.extend(Vin)
                                    
                                    # Clean MAP
                                    del self.Utxos[inp.transaction_hash][Vout]
                                    if len(self.Utxos[inp.transaction_hash]) == 0:
                                        del self.Utxos[inp.transaction_hash]
                                
                                # If low-memory is activated and at least one ./.utxos/uxtosplit file exists
                                # Same code as above but updating the respective uxtosplit file
                                elif self.lowMemory:
                                    Vins.append((inp.transaction_hash, inp.transaction_index))
                                
                                else:
                                    continue
                                    

                            # Outputs
                            Addrs_o = [addr.address for output in tx.outputs for addr in output.addresses]

                            # Build edge
                            self._buildEdge(Vins, Addrs_o)

                _print(f"File # {fn} successfully parsed")
                
                if self.buildRawEdges:
                    if self.upload:
                        success = save_Raw_Edges(self.Raw_Edges, fn, uploader=self.uploader, lm=self.lowMemory)
                        if success == "stop":
                            self.finish_tasks()
                            _print("Execution finished")
                            return self
                    else:
                        save_Raw_Edges(self.Raw_Edges, fn, location=self.buildRawEdges, lm=self.lowMemory)
                    # Reset list of raw edges
                    self.Raw_Edges = []
                    
                # Show loop duration after first iteration
                if t0:
                     loop_duration = show_delta_info(t0, loop_duration, blk_file, l)
                        
                # Must be the first iteration
                else:
                    loop_duration = show_delta_info(self.creationTime, loop_duration, blk_file, l)
                  
                t0 = datetime.now()
                
                _print(f"File # {fn} finished")
                
                # Print stats after each .blk file
                self.stats()
                    
            # Finish execution 
            self.finish_tasks()
            _print("Execution finished")
            return self
        
        except KeyboardInterrupt:
            self.logger.log("Keyboard interrupt...")
            self.finish_tasks()
            return self
        
        except SystemExit:
            self.logger.log("System exit...")
            self.finish_tasks()
            return self 
     
    
    # Activate to split up the Utxo dict in multiple files 
    # and start looping to find Input -> Output connections in order to safe memory
    def split_Utxos_and_activate_UtxosLooping(self):
        if not self.UtxosLoop:
            self.UtxosLoop = True
            
        # Save Utxos ...
        _print("Saving Utxo Split...")
        save_UtxoSplit(self.Utxos)
        
        # Start new Utxos dict
        self.Utxos = {}
            
        
    def finish_tasks(self):
        if self.buildRawEdges:
            if self.Utxos:
                save_Utxos(self.Utxos)
        else:
            self.save_GraphComponents()
        _print(f"Took {int((datetime.now()-self.creationTime).total_seconds()/60)} minutes since graph creation")
    
    def save_GraphComponents(self):
        _print("Saving components...")
        meta = [self.creationTime, self.lastBlTs, self.currBlHash, self.currTxHash]
        save_ALL(self.G, self.graphFormat, self.V, self.Utxos, self.MAP_V, meta)
        
    def load_GraphComponents(self):
        self.creationTime, self.lastBlTs, self.currBlHash, self.currTxHash = Meta
        if buildRawEdges:
            self.Raw_Edges = load_edge_list()
            return self.Raw_Edges
        else:
            self.G, self.V, self.Utxos, self.MAP_V, Meta = load_ALL()
            return self.G, self.V, self.Utxos, self.MAP_V, Meta
        
    
    def stats(self):
        if self.buildRawEdges:
            graphSize = sys.getsizeof(self.Raw_Edges)+sys.getsizeof(self.Utxos)
            _print("Edge list and Utxo mapping have {:>11,.0f} bytes".format(graphSize))
            _print("Edge list and Utxo mapping have {:>11,.0f} MB".format((graphSize)/1048576))
            _print("Utxo mapping has                {:>11,.0f} entries".format(len(self.Utxos)))
        else:
            _print("Graph has {:>16,} nodes".format(self.G.numberOfNodes()))
            _print("Graph has {:>16,} edges".format(self.G.numberOfEdges()))
            _print("Utxo mapping has {:>11,.0f} entries".format(len(self.Utxos)))
            graphSize = sys.getsizeof(self.V)+sys.getsizeof(self.MAP_V)+sys.getsizeof(self.Utxos)
            _print("Graph has {:>16,.0f} bytes".format(graphSize))
            _print("Graph has {:>16,.0f} MB".format((graphSize)/1048576))
        print_memory_info()
        
        
        
    #
    # INTERFACE
    #
    def follow_node(self, n, r=2, directed=True, plot=True):
    # Get local graph of specified Address n 
    # `r` specifies the number of times the algorithm is executed"""
        
        MAP = self.MAP_V
        MAP_r = self.MAP_V_r
        G = self.G
        
        if not directed:
            if not self._G:
                G = graph.Graph(self.G, directed=False)
                self._G = G
            else:
                G = self._G

        nodes = [int(MAP[n])]
        
        for i in range(r):
            print("--------------")
            _nodes = nodes[:]
            for node in _nodes:
                print(node)
                nodes.extend(list(G.iterNeighbors(int(node))))                  
                print(nodes)                    
                    
        nodes = list(set(nodes))
        sG = graphtools.subgraphFromNodes(G, nodes)
        
        print(f"Path with startpoint @{n} consists of {len(nodes)} nodes")
        addrs = [(i, MAP_r[str(i)], G.degree(i)) for i in nodes]
        print("{:^10} {:^35} {:^6}".format("Node ID", "Address", "Degree"))
        for i, j, d in addrs:
            print("{:^10} {:^35} {:^6}".format(i, j, d))
            
        maxnodes = 250
        
        if len(nodes) <= maxnodes and plot:
            viztasks.drawGraph(sG, node_size=[500]*len(nodes), label=True, labels=dict([(i,i) for i in sG.iterNodes()]))
            plt.show()
            
        elif len(m) > maxnodes:
            print("Community too large to plot (>100 nodes)")
        
        return sG
    
    def stream_neighborhood(n, r = 2, dicrected =True, endpoint = "http://localhost:8080/workspace1"):
        G = follow_node(n, r, directed, False)
        client = gephi.streaming.GephiStreamingClient(endpoint)
        client.exportGraph(G)
    
  
    def get_community(self, cix, plot=True):
    # Get local community by providing the Community Index Number cix
    
        MAP = self.MAP_V_r
        
        if not self._G:
            _G = graph.Graph(self.G, directed=False)
            self._G = _G
        else:
            _G = self._G
            
        if not self.communities:
            communities = community.detectCommunities(_G, algo=community.PLM(_G, True))
            self.communities = communities
        else:
            communities = self.communities

        m  = communities.getMembers(cix)
        sG = graphtools.subgraphFromNodes(_G, m)
        
        print(f"Community # {cix} has size of {len(m)} nodes")
        addrs = [(i, MAP[str(i)], _G.degree(i)) for i in m]
        print("{:^10} {:^35} {:^6}".format("Node ID", "Address", "Degree"))
        for i, j, d in addrs:
            print("{:^10} {:^35} {:^6}".format(i, j, d))
        
        maxnodes = 100
        
        if len(m) <= maxnodes and plot:
            viztasks.drawGraph(sG, node_size=[500]*len(m), label=True, labels=dict([(i,i) for i in sG.iterNodes()]))
            plt.show() 
            
        elif len(m) > maxnodes:
            print("Community too large to plot (>100 nodes)")
            
    
    def __repr__(self):
        return "Graph with {:>16,.0f} nodes and {:>12,} edges".format(self.G.numberOfNodes(),
                                                                      self.G.numberOfEdges())
    
