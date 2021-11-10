# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

# The bitcoin-transaction-parser is an extension of the python-blockchain-parser 
# that can be found here: https://github.com/alecalve/python-bitcoin-blockchain-parser.
# The python-blockchain-parser module is used to read from the raw .blk files. 
# To avoid installing level-db the indexing was removed from the library such that only
# the `get_unordered_blocks` function remains.


import os
import sys
import time

from termcolor import colored
from datetime import datetime
from networkit import *

from bitcoin_graph.blockchain_parser.blockchain import Blockchain
from bitcoin_graph.bqUploader import BQUploader, _print
from bitcoin_graph.logger import BlkLogger
from bitcoin_graph.helpers import *


# ----------
## BtcTxParser
#

class BtcTxParser:
    
    def __init__(self, edge_list=None, dl='~/.bitcoin/blocks', Utxos=None, 
                 targetpath=None, endTS=None, iC=None,
                 upload=False, credentials=None, table_id=None, dataset=None, raw=False,
                 cvalue=None, cblk=None
                ):
        self.creationTime = datetime.now()      # Creation time of `this`
        self.endTS        = endTS               # Timestamp of last block
        self.dl           = dl                  # Data location where blk files are stored
        self.Utxos        = Utxos or {}         # Mapping of Tx Hash => Utxo Indices => Output Addresses
        self.targetpath   = targetpath          # Path to store a list of raw edges as csv
        self.edge_list    = edge_list or []     # Raw Edges list
        self.logger       = BlkLogger()         # Logging module
        self.upload       = upload              # Bool to directly upload to GCP
        self.raw          = raw                 # Bool to activate parsing without mapping Utxos to Inputs
        self.cvalue       = cvalue              # Bool to activate collecting values
        self.cblk         = cblk                # Bool to activate collection blk file numbers
        if self.upload:
            self.creds    = credentials         # Path to google credentials json
            self.table_id = table_id            # GBQ table id
            self.dataset  = dataset             # GBQ data set name
            self.uploader = BQUploader(credentials=self.creds,
                                       table_id=self.table_id,
                                       dataset=self.dataset,
                                       logger=self.logger) # BigQuery uploader

        # Timestamp to datetime object
        if self.endTS:
            self.endTS=datetime.fromtimestamp(int(self.endTS))
        
        # Load existing Utxos mapping if path was specified
        if self.Utxos:
            self.Utxos = load_Utxos(self.Utxos)
        
        print("\nBtc Tx-Parser successfully initialized")
        time.sleep(1)
    
    
    def _buildEdge(self, u, v):
        for _u in set(u):
            # If collecting raw edges, then we need _index as vout too
            if self.raw:
                for _index, _v in enumerate(v):
                    
                    # Collecting values
                    if self.cvalue:
                            self.edge_list.append((self.currBl_s,self.currTxID,_u, _v, _index, self.Val[_index]))
                            
                    # ...no values    
                    else:
                        self.edge_list.append((self.currBl_s,self.currTxID,_u, _v, _index))
            
            # Input/Output mapped edges
            else:
                for _index, _v in enumerate(v):
                    if self.cvalue:
                        self.edge_list.append((self.currBl_s,self.currTxID,_u, _v, self.Val[_index]))

                    else:
                        self.edge_list.append((self.currBl_s,self.currTxID,_u, _v))
        return

    
 
    # Build Graph
    # Arguments: start-file sF, end-file eF, start-Tx sT, end-Tx eT
    def parse(self, sF, eF, sT, eT): 
        print("Start parsing...")
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
            self.l = len(blk_files)+file_number(sF) if sF else len(blk_files)
            self.t0, self.loop_duration, self.Val, self.cum_edges = None, [], None, 0
            
            # Loop through all .blk files
            for blk_file in blk_files:
                
                # Ensure to start with an empty array
                assert(len(self.edge_list) == 0)
                
                # Get integer of .blk filename (blk00001 => 1)
                self.fn = file_number(blk_file)
                
                # Log progress
                self.logger.log(f"Block File # {self.fn}/{self.l}")

                for block in blockchain.get_unordered_blocks(blk_file):
                    
                    # Keep track of processed blocks
                    self.currBlHash = block.hash

                    # Skip blocks younger than specified `end timestamp`
                    self.currBl   = block.header.timestamp
                    self.currBl_s = int(self.currBl.timestamp())
                    if self.endTS:                     
                        if self.currBl > self.endTS:
                            continue
                    
                    for tx in block.transactions:
                        
                        # Set `last-processed hash`
                        self.currTxID = tx.txid
                        
                        # ---
                        # Custom Start or End                      
                        # Try to start if start-transaction sT is reached
                        if start == False:
                            start = True if (sT == None or sT == self.currTxID) else False

                        # Try to stop execution afer last-transaction-hash is reached
                        if eT != None and start == True:
                            start = False if eT == self.currTxID else True
                            if start == False:
                                _print("End Tx reached")
                                _print("Execution terminated")
                                sys.exit(1)
                        # ---
                        
                        if start:
                            # Skip collecting outputs in low-memory mode (since not needed)
                            if not self.raw:
                                # Handle Outputs
                                outs={}
                                for o_index, output in enumerate(tx.outputs):
                                    outs[o_index] = [address.address for address in output.addresses]

                                self.Utxos[tx.txid] = outs

                            # Handle Inputs
                            Vins = []
                            for inp in tx.inputs:

                                # Coinbase Txs
                                if inp.transaction_hash == "0" * 64:
                                    # Build egde from ZERO to all Transaction output addresses
                                    Vins.append("0")
                                    
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
                                elif self.raw:
                                    Vins.append((inp.transaction_hash, inp.transaction_index))
                                
                                else:
                                    continue 

                            # Outputs
                            Addrs_o = [addr.address for output in tx.outputs for addr in output.addresses]
                            
                            # Output Value
                            if self.cvalue:
                                self.Val = [output.value for output in tx.outputs]

                            # Build edge
                            self._buildEdge(Vins, Addrs_o)

                # Safe or upload edge list
                success = save_edge_list(self)

                # If something failed, user can manually stop
                if success == "stop":
                    self.finish_tasks()
                    _print("Execution finished")
                    return self   
              
                # Reset list of raw edges
                self.edge_list = []
                
                # Reset t0 for next block
                self.t0 = datetime.now()                  

  
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
    
    # Final info prints
    def finish_tasks(self):
        if self.Utxos:
            save_Utxos(self.Utxos)
        print(f"\nTook {int((datetime.now()-self.creationTime).total_seconds()/60)} minutes since starting")
