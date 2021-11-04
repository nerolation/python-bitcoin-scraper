# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

from datetime import datetime
import os
import pickle
from termcolor import colored
import psutil
import re
import csv

# Helpers
#
now = datetime.now().strftime("%Y%m%d_%H%M%S")
def _print(s):
    print(f"{datetime.now().strftime('%H:%M:%S')}  -  {s}")

def get_date(folder="./output"):
    try:
        content = [fn for fn in os.listdir(folder)]
        dates = [datetime.strptime(fn, "%Y%m%d_%H%M%S") for fn in content]
        return dates[dates.index(max(dates))].strftime("%Y%m%d_%H%M%S")
    except:
        return 0
    
def get_csv_files(path):
        files = os.listdir(path)
        files = [f for f in files if f.startswith("raw") and f.endswith(".csv")]
        files = map(lambda x: os.path.join(path, x), files)
        return sorted(files)    
    
def save_Utxos(Utxos):
    check_folders()
    _print("Saving Utxos...")
    with open('./output/{}/Utxos.csv'.format(now), 'w') as f:  
        writer = csv.writer(f)
        writer.writerows(Utxos.items())

def save_UtxoSplit(Utxos, ix=None, location='./.utxos'):
    if not os.path.isdir(location):
        os.makedirs(location)
    if not os.path.isdir("{}/{}".format(location,now)):
        os.makedirs("{}/{}".format(location,now))
    if ix != None: 
        fn = ix
    else:
        fn = len(os.listdir("{}/{}".format(location,now)))
    with open('{}/{}/uxtosplit_{}.pkl'.format(location,now,fn), 'wb') as handle:
        pickle.dump(Utxos, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_UtxoSplits(location='./.utxos'):
    l = len(os.listdir("{}/{}".format(location,now)))
    for fn in range(l)[::-1]:
        with open('{}/{}/uxtosplit_{}.pkl'.format(location,now,fn), 'rb') as handle:
            #_print(f"providing {fn}")
            yield fn, pickle.load(handle)  
                        

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

def save_edge_list(rE, blkfile, location=None, uploader=None, raw=False, ax="", cblk=False):
    rE = rE[0:1000]
    
    # If edges contain timestamp
    try:
        t_0 = datetime.fromtimestamp(int(rE[0][0])).strftime("%d.%m.%Y")
        t_1 = datetime.fromtimestamp(int(rE[-1][0])).strftime("%d.%m.%Y")
        if uploader:
            _print("Data ranges from {} to {}".format(t_0, t_1))
    except:
        pass
    
    # If collecting blk numbers is activated, then append it to every edge
    if cblk:
        rE = list(map(lambda x: (x) + (blkfile,), rE))
    
    # If raw edges mode is activated, flatten each line of rE
    if raw:
        # if third entry is a tuple then transaction != coinbase transaction
        rE = [(*row[0:2],*row[2],*row[3:]) if type(row[2]) == tuple else (*row[0:3],*row[2:]) for row in rE]
        ax = "_raw"
        
    # Direct upload to Google BigQuery without local copy
    if uploader:
        _print("Batch contains {:,} edges".format(len(rE)))
        _print("Start uploading ...")
        success = uploader.upload_data(rE, cblk=cblk)
        if success and success != "stop":
            _print("Upload successful")
        return success

    
    # Store locally
    else:
        _print("File @ raw_blk_{}{}.csv contains {:,} edges".format(blkfile, ax, len(rE)))
        _print("Saving raw edges...")
        if not os.path.isdir('{}/output'.format(location)):
            os.makedirs('{}/output'.format(location))
        if not os.path.isdir('{}/output/{}/rawedges/'.format(location,now)):
            os.makedirs('{}/output/{}/rawedges'.format(location,now))
        with open("{}/output/{}/rawedges/raw_blk_{}{}.csv".format(location, now, blkfile, ax),"w",newline="") as f:
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
                 
def used_ram():
    m = psutil.virtual_memory()
    return m.percent
    
        
def estimate_end(loopduration, curr_file, total_files):
    avg_loop = int(sum(loopduration[-15:])/len(loopduration[-15:]))
    delta_files = total_files - curr_file
    _estimate = datetime.fromtimestamp(datetime.now().timestamp() + delta_files * avg_loop)
    return "Estimated end:  " +  _estimate.strftime("%d.%m  |  %H:%M:%S")

def file_number(s):
    match = re.search("([0-9]{5})", s).group()
    if match == "00000":
        return 0
    else:
        return int(match.lstrip("0"))    

def get_table_schema(cls, cblk):
    if len(cls) == 3:
        c = [ {'name': '{}'.format(cls[0]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[1]), 'type': 'STRING'},
              {'name': '{}'.format(cls[2]), 'type': 'STRING'}
            ]
    
    elif len(cls) == 6:
        c = [ {'name': '{}'.format(cls[0]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[1]), 'type': 'STRING'},
              {'name': '{}'.format(cls[2]), 'type': 'STRING'},
              {'name': '{}'.format(cls[3]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[4]), 'type': 'STRING'},
              {'name': '{}'.format(cls[5]), 'type': 'INTEGER'}
            ]
        
    elif len(cls) >= 7:
        c = [ {'name': '{}'.format(cls[0]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[1]), 'type': 'STRING'},
              {'name': '{}'.format(cls[2]), 'type': 'STRING'},
              {'name': '{}'.format(cls[3]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[4]), 'type': 'STRING'},
              {'name': '{}'.format(cls[5]), 'type': 'INTEGER'},
              {'name': '{}'.format(cls[6]), 'type': 'INTEGER'}
            ]
    if cblk:
        c.append({'name': '{}'.format("blk_file_nr"), 'type': 'INTEGER'})
        
    return c

def show_delta_info(t0, loop_duration, blk_file, l):
    delta = (datetime.now()-t0).total_seconds()
    if delta > 5:
        _print(f"File @ `{blk_file}` took {int(delta)} seconds")
        loop_duration.append(delta)
        _print(f"Average duration of {int(sum(loop_duration)/len(loop_duration))} seconds per .blk file")
        _print(estimate_end(loop_duration, file_number(blk_file), l))
        return loop_duration
            