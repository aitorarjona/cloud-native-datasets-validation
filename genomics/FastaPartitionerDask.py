import dask
import dask.array as da
import dask.bag as db
import dask.dataframe as dd
from dask import delayed
import numpy as np
import re
from distributed import LocalCluster, Client

def read_metadata_partitions(arr):
    pattern = re.compile(r">")
    res = []
    prev = 0
    next = 0
    for el in arr:
        next = next + len(el)
        if pattern.match(el):
            res.append([">", prev, next])
        prev = next
    return [res[-1][-1],res]

    

if __name__ == '__main__':
    
    df = db.read_text('fasta.fasta').repartition(npartitions=2)
    df = df.map_partitions(lambda x: read_metadata_partitions(x))
    arr = df.compute()
    print(arr)
    
