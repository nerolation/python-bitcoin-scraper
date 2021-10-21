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
from bitcoin_graph.BQUploader import BQUploader, _print
from bitcoin_graph.logger import BlkLogger
from bitcoin_graph.helpers import *


# ----------
## BtcTxParser
#

class BtcTxParser:
    
    def __init__(self, edge_list=None, Meta=None, dl='~/.bitcoin/blocks', Utxos=None, 
                 localpath=None, withTS=None, endTS=None, clusterInputs=None, iC=None,
                 upload=False, credentials=None, table_id=None, dataset=None, lowMemory=False,
                 collectValue=None
                ):
        self.endTS        = endTS               # Timestamp of last block
        self.dl           = dl                  # Data location where blk files are stored
        self.Utxos        = Utxos or {}         # Mapping of Tx Hash => Utxo Indices => Output Addresses
        self.localpath    = localpath           # Path to store a list of raw edges as csv
        self.withTS       = withTS              # Bool value to decide whether timestamps are collected
        self.edge_list    = edge_list or []     # Raw Edges list
        self.logger       = BlkLogger()         # Logging module
        self.upload       = upload              # Bool to directly upload to GCP
        self.lowMemory    = lowMemory           # Bool to activate low-memory-consumption mode
        self.collectValue = collectValue        # Bool to activate collecting values
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

        # Timestamp to datetime object
        if self.endTS:
            self.endTS=datetime.fromtimestamp(int(self.endTS))
        
        # Load existing Utxos mapping if path was specified
        if self.Utxos:
            self.Utxos = load_Utxos(self.Utxos)
        
        _print("New Btc Tx-Parser initialized")
        time.sleep(1)
    
    
    def _buildEdge(self, u, v, Val=None):
        for _u in set(u):
            # Collect Vout in low-memory mode
            if self.lowMemory:
                for _index, _v in enumerate(v):
                    if self.withTS and self.collectValue:
                        try:
                            self.edge_list.append((self.lastBlTs_s,self.currTxHash,_u, _v, _index, Val[_index]))
                        
                        # If len of Val != len of v then there is some crappy output script in the output
                        except IndexError:
                            print(self.currTxHash)
                            print(list(enumerate(v)))
                            print(Val)
                            print(_index)
                        
                    elif self.withTS:
                        self.edge_list.append((self.lastBlTs_s,self.currTxHash,_u, _v, _index))
                        
                    elif self.collectValue:
                        self.edge_list.append((self.currTxHash,_u, _v, _index, Val[_index]))
                        
                    else:
                        self.edge_list.append((self.currTxHash,_u, _v, _index))
            else:
                for _v in v:
                    if self.withTS:
                        self.edge_list.append((self.lastBlTs_s,_u, _v))
                    else:
                        self.edge_list.append((_u, _v))
        return

    
 
    # Build Graph
    # Arguments: start-file sF, end-file eF, start-Tx sT, end-Tx eT
    def parse(self, sF, eF, sT, eT): 
        _print("Start building...")
        try:
            # Instantiate the Blockchain by giving the path to the directory
            # containing the .blk files created by bitcoind
            blockchain = Blockchain(os.path.expanduser(self.dl))
            
            # Set start to True if no Start transaction is provided
            start = True if sT == None else False
            
            # Receive list of .blk files
            blk_files = blockchain.get_blk_files(sF, eF)
            
            # l = number of .blk files
            # t0 = time iteration beginns
            # loop_duration = list of delta times of iterations
            # Value received by output
            l, t0, loop_duration, Val = len(blk_files), None, [], None
            
            # Loop through all .blk files
            for blk_file in blk_files:
                
                # Ensure to start with an empty array
                assert(len(self.edge_list) == 0)
                
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
                            
                            # Output Value
                            if self.collectValue:
                                Val = [output.value for output in tx.outputs]

                            # Build edge
                            self._buildEdge(Vins, Addrs_o, Val)

                _print(f"File # {fn} successfully parsed")
                
                # Upload if uploading is activated
                if self.upload:
                    success = save_edge_list(self.edge_list, fn, uploader=self.uploader, lm=self.lowMemory)
                    
                    # If something failed, user can manually stop
                    if success == "stop":
                        self.finish_tasks()
                        _print("Execution finished")
                        return self
                    
                # Else, store edge list locally
                else:
                    save_edge_list(self.edge_list, fn, location=self.localpath, lm=self.lowMemory)
                    
                # Reset list of raw edges
                self.edge_list = []
                    
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
     
    def finish_tasks(self):
        if self.Utxos:
            save_Utxos(self.Utxos)
        _print(f"Took {int((datetime.now()-self.creationTime).total_seconds()/60)} minutes since starting")
   
    
    def stats(self):
        csize = sys.getsizeof(self.edge_list)+sys.getsizeof(self.Utxos)
        _print("Edge list and Utxo mapping have {:>11,.0f} bytes".format(csize))
        _print("Edge list and Utxo mapping have {:>11,.0f} MB".format((csize)/1048576))
        _print("Utxo mapping has                {:>11,.0f} entries".format(len(self.Utxos)))
        print_memory_info()
        
        
        
    
