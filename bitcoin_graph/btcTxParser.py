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
from datetime import datetime

from bitcoin_graph.blockchain_parser.blockchain import Blockchain
from bitcoin_graph.uploader import Uploader, _print
from bitcoin_graph.logger import BlkLogger
from bitcoin_graph.helpers import _print, save_edge_list, file_number, print_output_header


# ----------
## BtcTxParser
#

class BtcTxParser:

    def __init__(self, edge_list=None, dl='~/.bitcoin/blocks', 
                 targetpath=None, endTS=None, iC=None, upload=False, 
                 credentials=None, dataset=None, table_id=None, project=None, 
                 cvalue=None, cblk=None, use_parquet=False, 
                 upload_threshold=None, bucket=None, multi_p=False
                ):
        self.creationTime = datetime.now()      # Creation time of `this`
        self.endTS        = endTS               # Timestamp of last block
        self.dl           = dl                  # Data location where blk files are stored
        self.targetpath   = targetpath          # Path to store a list of raw edges as csv
        self.edge_list    = edge_list or []     # Raw Edges list
        self.logger       = BlkLogger()         # Logging module
        self.upload       = upload              # Bool to directly upload to GCP
        self.cvalue       = cvalue              # Bool to activate collecting values
        self.cblk         = cblk                # Bool to activate collection blk file numbers
        self.multi_p      = multi_p             # Bool to activate multiprocessing
        self.use_parquet = use_parquet         # Use parquet format
        if self.upload:
            self.creds       = credentials         # Path to google credentials json
            self.project     = project
            self.table_id    = table_id            # GBQ table id
            self.dataset     = dataset             # GBQ data set name
            self.parq_thres  = upload_threshold    # Threshold when parquet files will be uploaded 
            self.bucket      = bucket              # Bucket name to store parquet files
            self.uploader    = Uploader(credentials=self.creds,
                                        project    = self.project,
                                        dataset    = self.dataset,
                                        table_id   = self.table_id,
                                        logger     = self.logger,
                                        pthreshold = self.parq_thres,
                                        bucket     = self.bucket,
                                        multi_p    = self.multi_p
                                       ) # BigQuery uploader

        # Timestamp to datetime object
        if self.endTS:
            self.endTS=datetime.fromtimestamp(int(self.endTS))     
        
        print("\nBtc Tx-Parser successfully initialized")
        time.sleep(1)
    
    
    def _buildEdge(self, u, v, values):
        '''Build edge by looping over the inputs `u` and the outputs `v`
           and creating edges with every combination.
        '''
        for _u in set(u):
            for _index, _v in enumerate(v):

                # Collecting values
                if self.cvalue:
                    self.edge_list.append((self.currBl_s,
                                           self.currTxID,
                                           _u, 
                                           _v, 
                                           _index, 
                                           values[_index]))

                # ...no values    
                else:
                    self.edge_list.append((self.currBl_s,
                                           self.currTxID,
                                           _u, 
                                           _v, 
                                           _index))
        return None

    # Build Graph
    def parse(self, sF, eF, sT, eT): 
        '''Parising function that starts the parsing process.
           Arguments: start file `sF`, end file `eF`, start tx `sT` and a end tx `eT`.
        '''
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
            self.l = len(blk_files)+file_number(sF)-1 if sF else len(blk_files)-1
            self.t0, self.loop_duration, self.Val, self.cum_edges = None, [], None, 0
            print_output_header(self)
            
            # Loop through all .blk files
            for blk_file in blk_files:
                
                # Ensure to start with an empty array
                if not self.use_parquet:
                    assert(len(self.edge_list) == 0)
                
                # Get integer of .blk filename (blk00001 => 1)
                self.fn = file_number(blk_file)
                
                # Log progress
                self.logger.log(f"Block File # {self.fn}/{self.l}")

                for block in blockchain.get_unordered_blocks(blk_file):
                    
                    # Keep track of processed blocks
                    self.currBlHash = block.hash

                    # Skip blocks younger than specified `end timestamp`
                    self.currBl_s = block.header.timestamp
                    self.currBl = datetime.utcfromtimestamp(self.currBl_s)
                    if self.endTS:                     
                        if self.currBl > self.endTS:
                            continue
                    
                    for tx in block.transactions:
                        
                        # Set `last-processed tx id`
                        self.currTxID = tx.txid
                        
                        # ---
                        # Custom Start or End                      
                        # Try to start if start-transaction sT is reached
                        if start == False:
                            start = True if (sT == None or sT == self.currTxID) else False

                        # Try to stop execution afer last-transaction-id is reached
                        if eT != None and start == True:
                            start = False if eT == self.currTxID else True
                            if start == False:
                                _print("End Tx reached")
                                _print("Execution terminated")
                                sys.exit(1)
                        # ---
                        
                        # Start variable used for custom starts
                        if start:

                            # Handle Inputs
                            Vins = []
                            for inp in tx.inputs:

                                # Coinbase Txs
                                if inp.transaction_hash == "0" * 64:
                                    # Build egde from ZERO to all Transaction output addresses
                                    Vins.append("0")
                               
                                # Append transaction id and vout 
                                else:
                                    Vins.append((inp.transaction_hash, int(inp.transaction_index)))

                            # Outputs and Values
                            Outs = []
                            Vals = []
                            for output in tx.outputs:
                                # Multisigs might contain multiple addresses
                                for address in output.addresses:
                                    Outs.append(address.address)
                                    Vals.append(output.value)

                            # Build edge
                            self._buildEdge(Vins, Outs, Vals)
                
                if start:
                    if not self.use_parquet:
                        _print(f"blk file nr. {self.fn} successfully parsed...", end='\r')
                    # Safe/upload and reset edge list and then reset it
                    success = save_edge_list(self)

                    # If something failed, user can manually stop
                    if success == "stop":
                        self.finish_tasks()
                        _print("Execution finished")
                        return self   
                
                # Reset t0 for next block
                self.t0 = datetime.now()                  

  
            # Finish execution 
            _print("Parsing finished\n")
            self.finish_tasks()
            return self
        
        except KeyboardInterrupt:
            self.logger.log("Keyboard interrupt...\n")
            self.finish_tasks()
            return self
        
        except SystemExit:
            self.logger.log("System exit...\n")
            self.finish_tasks()
            return self 
    
    # Final info prints
    def finish_tasks(self):
        # Make sure everything is saved
        if len(self.edge_list) > 0:
            success = save_edge_list(self)
        
        # Create end file for multiprocessing
        if self.multi_p:
            with open("./.temp/end_multiprocessing.txt", "w") as file:
                file.write("True")
                
        execution_time = int((datetime.now() \
                              - self.creationTime).total_seconds()/60)
        _print(f"Took {execution_time} minutes since starting\n")
