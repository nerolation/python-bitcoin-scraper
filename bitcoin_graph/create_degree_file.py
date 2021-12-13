# Copyright (C) Anton Wahrstätter 2021

# This file is part of python-bitcoin-graph which was forked from python-bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of python-bitcoin-graph, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

# The create_degree_file provides a function to prepare the data for calculating and 
# visualizing the degree distribution

# Prepare degrees for calculating power-law coefficient
import pandas as pd
import os

def create_degree_file(bq_project="wu-btcgraph", bq_dataset="btc"):
    path = input("input path to store degree-file: ")
    
    # load tables
    bq_table = ["bitcoin_degree_distribution_total",
                "bitcoin_degree_distribution_in_degree",
                "bitcoin_degree_distribution_out_degree"]

    suffixes = ["_total.txt", "_in.txt", "_out.txt"]
    
    for index, _ in enumerate(range(3)):
        print("{}/3".format(index+1))
        degrees=[]
        query = """
                SELECT    *
                FROM      `{}.{}.{}`
                """.format(bq_project, bq_dataset, bq_table[index])
        df = pd.read_gbq(query, bq_project)
        for u, (i, j) in enumerate(df.iterrows()):
            degrees.extend(j["frequency"] * [str(j["degree"])])

        with open(path + suffixes[index], "w") as file:
            file.write("\n".join(degrees))
      
    
create_degree_file()
        

