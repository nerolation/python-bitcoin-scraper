# Copyright (C) 2015-2016 The bitcoin-blockchain-parser developers
#
# This file is part of bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of bitcoin-blockchain-parser, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

import os
import mmap
import struct
import stat
import re

from datetime import datetime
from termcolor import colored
from .block import Block


# Constant separating blocks in the .blk files
BITCOIN_CONSTANT = b"\xf9\xbe\xb4\xd9"


def get_files(path):
    """
    Given the path to the .bitcoin directory, returns the sorted list of .blk
    files contained in that directory
    """
    if not stat.S_ISDIR(os.stat(path)[stat.ST_MODE]):
        return [path]
    files = os.listdir(path)
    files = [f for f in files if f.startswith("blk") and f.endswith(".dat")]
    files = map(lambda x: os.path.join(path, x), files)
    return sorted(files)


def get_blocks(blockfile):
    """
    Given the name of a .blk file, for every block contained in the file,
    yields its raw hexadecimal value
    """
    with open(blockfile, "rb") as f:
        if os.name == 'nt':
            size = os.path.getsize(f.name)
            raw_data = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
        else:
            # Unix-only call, will not work on Windows, see python doc.
            raw_data = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        length = len(raw_data)
        offset = 0
        block_count = 0
        while offset < (length - 4):
            if raw_data[offset:offset+4] == BITCOIN_CONSTANT:
                offset += 4
                size = struct.unpack("<I", raw_data[offset:offset+4])[0]
                offset += 4 + size
                block_count += 1
                yield raw_data[offset-size:offset]
            else:
                offset += 1
        raw_data.close()


def get_block(blockfile, offset):
    """Extracts a single block from the blockfile at the given offset"""
    with open(blockfile, "rb") as f:
        f.seek(offset - 4)  # Size is present 4 bytes before the db offset
        size, = struct.unpack("<I", f.read(4))
        return f.read(size)
    
def file_number(s):
    match = re.search("([0-9]{5})", s).group()
    if match == "00000":
        return 0
    else:
        return int(match.lstrip("0"))

def estimate_end(loopduration, curr_file, total_files):
    avg_loop = int(sum(loopduration)/len(loopduration))
    delta_files = total_files - curr_file
    _estimate = datetime.fromtimestamp(datetime.now().timestamp() + delta_files * avg_loop)
    return colored(f"{datetime.now().strftime('%H:%M:%S')}  -  Estimated end:  " +  _estimate.strftime("%d.%m  |  %H:%M:%S"), "green")


class Blockchain(object):
    """Represents the blockchain contained in the series of .blk files
    maintained by bitcoind.
    """

    def __init__(self, path):
        self.path = path
        self.blockIndexes = None
        self.indexPath = None

    def get_unordered_blocks(self, customStart=None, customEnd=None, logger=None):
        """Yields the blocks contained in the .blk files as is,
        without ordering them according to height.
        """
        start = True
        t0 = None
        loop_duration = []
        
        if customStart:
            start=False
            
        blk_files = get_files(self.path)
        l = len(blk_files)
        for blk_file in blk_files:
            print(colored(f"{datetime.now().strftime('%H:%M:%S')}  -  Block File # {file_number(blk_file)}/{l}", "green"))
            if logger != None:
            	logger.log(f"{datetime.now().strftime('%H:%M:%S')}  -  Block File # {file_number(blk_file)}/{l}")
            if t0:
                delta = (datetime.now()-t0).total_seconds()
                if delta > 5:
                    print(f"{datetime.now().strftime('%H:%M:%S')}  -  File @ `{blk_file}` took {int(delta)} seconds", "green")
                    loop_duration.append(delta)
                    print(f"{datetime.now().strftime('%H:%M:%S')}  -  Average duration of {int(sum(loop_duration)/len(loop_duration))} seconds per .blk file", "green")
                    print(estimate_end(loop_duration, file_number(blk_file), l))
            t0 = datetime.now()

            if str(customStart) in blk_file:
                start=True
            if str(customEnd) in blk_file:
                start=False

            if start:
                print(f"\nProcessing {blk_file}")
                for raw_block in get_blocks(blk_file):
                    yield Block(raw_block)
