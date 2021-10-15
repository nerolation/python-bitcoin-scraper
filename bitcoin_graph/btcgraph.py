import os
import sys
import re
import itertools
import time
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import psutil

from termcolor import colored
from datetime import datetime
from networkit import *

from bitcoin_graph.blockchain_parser.blockchain import Blockchain
from bitcoin_graph.input_heuristic import InputHeuristic
from bitcoin_graph.bquploader import bqUpLoader


#
# Helpers
#

def _print(s):
    print(f"{datetime.now().strftime('%H:%M:%S')}  -  {s}")
    
def get_date(folder="./output"):
    try:
        content = [fn for fn in os.listdir(folder)]
        dates = [datetime.strptime(fn, "%Y%m%d_%H%M%S") for fn in content]
        return dates[dates.index(max(dates))].strftime("%Y%m%d_%H%M%S")
    except:
        return 0

def save_Utxos(Utxos):
    check_folders()
    _print("Saving Utxos...")
    with open('./output/{}/Utxos.csv'.format(now), 'w') as f:  
        writer = csv.writer(f)
        writer.writerows(Utxos.items())

def load_Utxos(path='./output/{}/'.format(get_date())):
    _print("Loading Utxos...")
    with open(path + "/Utxos.csv", 'r') as f:  
        return {a:eval(b) for a,b in csv.reader(f)}
    
def save_MAP_V(MAP_V):
    _print("Saving MAP_V...")
    with open('./output/{}/MAP_V.csv'.format(now), 'w') as f:  
        writer = csv.writer(f)
        writer.writerows(MAP_V.items())

def load_MAP_V():
    _print("Loading MAP_V...")
    with open('./output/{}/MAP_V.csv'.format(get_date()), 'r') as f:  
        return {a:b for a,b in csv.reader(f)}

def save_V(V):
    _print("Saving V...")
    pickle.dump(V, open( "./output/{}/V.pkl".format(now), "wb"))

def load_V():
    _print("Loading V...")
    return pickle.load(open("./output/{}/V.pkl".format(get_date()), "rb"))

def save_G(G, graphFormat):
    _print("Saving G...")
    if graphFormat == "binary":
        _print("Saving G in networkit binary format...")
        writeGraph(G,"./output/{}/G.graph".format(now), Format.NetworkitBinary, chunks=16, NetworkitBinaryWeights=0)
        _print("Additionally store graph in edge list format? (y/n)")
        if input() != "n":
            _print("Saving G in edge-list format...")
            writeGraph(G,"./output/{}/G_raw.edges".format(now), Format.EdgeListSpaceZero)              
    else:
        _print("Saving G in edge-list format...")
        writeGraph(G,"./output/{}/G_raw.edges".format(now), Format.EdgeListSpaceZero)
 
    
def load_G():
    _print("Loading G...")
    return readGraph("./output/{}/G.graph".format(get_date()), Format.NetworkitBinary)

def save_Meta(M):
    _print("Saving Metadata...")
    pickle.dump(M, open( "./output/{}/Metadata.meta".format(now), "wb"))

def load_Meta():
    _print("Loading Metadata...")
    return pickle.load(open("./output/{}/Metadata.meta".format(get_date()), "rb"))

def save_Raw_Edges(rE, blkfile, location=None, uploader=None):
    # If edges contain timestamp
    if len(rE[0]) == 3:
        t_0 = datetime.fromtimestamp(int(rE[0][0])).strftime("%d.%m.%Y")
        t_1 = datetime.fromtimestamp(int(rE[-1][0])).strftime("%d.%m.%Y")
        if uploader:
            _print("Data ranges from {} to {}".format(t_0, t_1))
        else:
             _print("File @ raw_blk_{}.csv ranges from {} to {}".format(blkfile, t_0, t_1))
    
    # Direct upload to Google BigQuery without local copy
    if uploader:
        _print("Batch contains {:,} edges".format(len(rE)))
        uploader.upload_data(rE)
        _print("Upload successful")
    
    # Store locally
    else:
        _print("File @ raw_blk_{}.csv contains {:,} edges".format(blkfile, len(rE)))
        _print("Saving raw edges...")
        if not os.path.isdir('{}/output'.format(location)):
            os.makedirs('{}/output'.format(location))
        if not os.path.isdir('{}/output/{}/rawedges/'.format(location,now)):
            os.makedirs('{}/output/{}/rawedges'.format(location,now))
        with open("{}/output/{}/rawedges/raw_blk_{}.csv".format(location, now, blkfile),"w",newline="") as f:
            cw = csv.writer(f,delimiter=",")
            cw.writerows(rE)
        _print("Saving successful")

def load_edge_list():
    _print("Loading edge list...")
    for chunk in pd.read_csv("./output/{}/G_raw.G".format(get_date()), 
                             header=None, chunksize=1e6):
        _print("more ...")
        yield list(chunk.to_records(index=False))

def check_folders():
    if not os.path.isdir('./output/'):
        _print("Creating output folder...")
        os.makedirs('./output')
    if not os.path.isdir('./output/{}/'.format(now)):
        _print("Creating output/{} folder...".format(now))
        os.makedirs('./output/{}/'.format(now))
        
def save_ALL(G,graphFormat,V,Utxos,MAP_V,Meta):
    check_folders()
    save_G(G, graphFormat)
    save_V(V)
    save_Utxos(Utxos)
    save_MAP_V(MAP_V)
    save_Meta(Meta)
    _print("Saving successful")
    
def load_ALL():
    G=load_G()
    V=load_V()
    Utxos=load_Utxos()
    MAP_V=load_MAP_V()
    Meta=load_Meta()
    _print("Loading successful")
    return G,V,Utxos,MAP_V,Meta

def MAP_V_r(m):
    return dict([(i, a) for a, i in m.items()])

def print_memory_info():
    m = psutil.virtual_memory()
    process = psutil.Process(os.getpid())
    col = "red" if m.total*0.75 < m.used else None
    _print(colored("---  -------   ----------------------------", col))
    _print(colored("--> {:>5,.1f} GB   total memory".format(m.total/1073741824), col))
    _print(colored("--> {:>5,.1f} GB   of memory available".format(m.available/1073741824), col))
    _print(colored("--> {:>5,.1f} GB   memory used".format(m.used/1073741824), col))
    _print(colored("--> {:>5,.1f} GB   memory used by this process".format(process.memory_info().rss/1073741824), col)) 
    _print(colored("--> {:>5,.1f}  %   of memory used".format(m.percent), col))
    _print(colored("--> {:>5,.1f}  %   of memory available".format(100-m.percent), col))
    _print(colored("---  -------   ----------------------------", col))
                 

def estimate_end(loopduration, curr_file, total_files):
    avg_loop = int(sum(loopduration)/len(loopduration))
    delta_files = total_files - curr_file
    _estimate = datetime.fromtimestamp(datetime.now().timestamp() + delta_files * avg_loop)
    return "Estimated end:  " +  _estimate.strftime("%d.%m  |  %H:%M:%S")

def file_number(s):
    match = re.search("([0-9]{5})", s).group()
    if match == "00000":
        return 0
    else:
        return int(match.lstrip("0"))    
                 

def show_delta_info(t0, loop_duration, blk_file, l):
    delta = (datetime.now()-t0).total_seconds()
    if delta > 5:
        _print(f"File @ `{blk_file}` took {int(delta)} seconds")
        loop_duration.append(delta)
        _print(f"Average duration of {int(sum(loop_duration)/len(loop_duration))} seconds per .blk file")
        _print(estimate_end(loop_duration, file_number(blk_file), l))
        return loop_duration
                 
    
 # Logger   
class BlkLogger:
    def __init__(self):
        if not os.path.isdir('logs/'):
            _print("Creating logs folder...")
            os.makedirs('logs')
            
            
    def log(self, s):
        ts = datetime.now().strftime("%Y-%m-%d  |  %H:%M:%S ")
        with open("logs/logs.txt", "a") as logfile:
            logfile.write(ts + s + "\n")
            
# ----------
## BtcGraph
#

class BtcGraph:
    
    def __init__(self, G=None, V=None, Utxos=None, MAP_V=None, Raw_Edges=None, Meta=None,
                 dl='~/.bitcoin/blocks', buildRawEdges=False, withTS=None, endTS=None, 
                 graphFormat="binary", upload=False, credentials=None, table_id=None, dataset=None,
                 clusterInputs=False, iC=None
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
        if self.upload:
            self.creds    = credentials     # Path to google credentials json
            self.table_id = table_id        # GBQ table id
            self.dataset  = dataset         # GBQ data set name
            self.uploader = bqUpLoader(credentials=self.creds,
                                       table_id=self.table_id,
                                       dataset=self.dataset)    # BigQuery uploader
            
        
        # Meta data
        self.lastTxHash   = Meta[3] if Meta else None # Last Tx Hash processed
        self.lastBlHash   = Meta[2] if Meta else None # Last Block Hash processed
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
            for _u in u:
                for _v in v:
                    if self.withTS:
                        self.Raw_Edges.append((self.lastBlTs_s,_u, _v))
                    else:
                        self.Raw_Edges.append((_u, _v))
            return

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

                    # Skip blocks younger than specified `end timestamp`
                    self.lastBlTs   = block.header.timestamp
                    self.lastBlTs_s = int(self.lastBlTs.timestamp())
                    if self.endTS:                     
                        if self.lastBlTs > self.endTS:
                            continue

                    # Keep track of processed blocks
                    self.lastBlHash = block.hash

                    for tx in block.transactions:
                        if start:
                            
                            # Handle Outputs
                            outs={}
                            for o_index, output in enumerate(tx.outputs):
                                outs[o_index] = [address.address for address in output.addresses]

                            self.Utxos[tx.hash] = outs

                            # Handle Inputs
                            for inp in tx.inputs:

                                # Coinbase Txs
                                if inp.transaction_hash == "0" * 64:
                                    # Outputs
                                    Addrs_o = [addr.address for output in tx.outputs for addr in output.addresses]

                                    # Build egde from ZERO to all Transaction output addresses
                                    self._buildEdge(["00"], Addrs_o)

                                # If possible to connect inputs -> prev. outputs
                                elif inp.transaction_hash in self.Utxos:

                                    # Inputs (Vin)
                                    Vout = inp.transaction_index
                                    Vin = self.Utxos[inp.transaction_hash][Vout]

                                    # Outputs
                                    Addrs_o = [addr.address for output in tx.outputs for addr in output.addresses]

                                    # Build edge
                                    self._buildEdge(Vin, Addrs_o)

                                    # Clean MAP
                                    del self.Utxos[inp.transaction_hash][Vout]
                                    if len(self.Utxos[inp.transaction_hash]) == 0:
                                        del self.Utxos[inp.transaction_hash]
              
                        # Set `last-processed hash`
                        self.lastTxHash = tx.hash
                        
                        # Try to start if start-transaction sT is reached
                        if start == False:
                            start = True if (sT == None or sT == self.lastTxHash) else False
                            
                        # Try to stop execution afer last-transaction-hash is reached
                        if eT != None and start == True:
                            start = False if eT == self.lastTxHash else True
                            if start == False:
                                _print("End Tx reached")
                                _print("Execution terminated")
                                sys.exit(1)

                _print(f"File # {fn} successfully parsed")
                
                # Free memory
                self.Utxos = dict(self.Utxos)
                                
                # Print stats after each .blk file
                self.stats()
                if self.buildRawEdges:
                    if self.upload:
                        _print("Start uploading ...")
                        save_Raw_Edges(self.Raw_Edges, fn, uploader=self.uploader)
                    else:
                        save_Raw_Edges(self.Raw_Edges, fn, location=self.buildRawEdges)
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
     
        
    def finish_tasks(self):
        if self.buildRawEdges:
            save_Utxos(self.Utxos)
        else:
            self.save_GraphComponents()
            _print(f"Took {int((datetime.now()-self.creationTime).total_seconds()/60)} minutes since graph creation")

    
    def save_GraphComponents(self):
        _print("Saving components...")
        meta = [self.creationTime, self.lastBlTs, self.lastBlHash, self.lastTxHash]
        save_ALL(self.G, self.graphFormat, self.V, self.Utxos, self.MAP_V, meta)
        
    def load_GraphComponents(self):
        self.creationTime, self.lastBlTs, self.lastBlHash, self.lastTxHash = Meta
        if buildRawEdges:
            self.Raw_Edges = load_edge_list()
            return self.Raw_Edges
        else:
            self.G, self.V, self.Utxos, self.MAP_V, Meta = load_ALL()
            return self.G, self.V, self.Utxos, self.MAP_V, Meta
        
    
    def stats(self):
        if self.buildRawEdges:
            graphSize = sys.getsizeof(self.Raw_Edges)+sys.getsizeof(self.Utxos)
            _print("Edge list and Utxo mapping have {:>16,.0f} bytes".format(graphSize))
            _print("Edge list and Utxo mapping have {:>16,.0f} mb".format((graphSize)/1048576))
        else:
            _print("Graph has {:>16,} nodes".format(self.G.numberOfNodes()))
            _print("Graph has {:>16,} edges".format(self.G.numberOfEdges()))
            graphSize = sys.getsizeof(self.V)+sys.getsizeof(self.MAP_V)+sys.getsizeof(self.Utxos)
            _print("Graph has {:>16,.0f} bytes".format(graphSize))
            _print("Graph has {:>16,.0f} mb".format((graphSize)/1048576))
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
    
