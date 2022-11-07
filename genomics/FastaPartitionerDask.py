import dask
import dask.array as da
import dask.bag as db
import dask.dataframe as dd
from dask import delayed
import numpy as np
import re
import multiprocessing
import subprocess as sp
from tempfile import NamedTemporaryFile
import s3fs
from itertools import islice
import numpy as np
from distributed import Client


class FastaPartitionerDask():
    def __init__(self,fasta_file,npartitions):
        self.fasta_file = fasta_file
        self.npartitions = npartitions
       
    def _read_metadata_partitions(self,arr):
        pattern = re.compile(r">")
        res = []
        prev = 0
        next = 0
        for el in arr:
            next = next + len(el)
            if pattern.match(el):
                res.append([">", prev, next, el])
            prev = next
        return [[next,res]]

    
            
    def _generate_chunks_no_correction(self,arr):
        f = NamedTemporaryFile(mode='w+', delete=False)
        f.write(' '.join(arr))
        f.close()

        return [f.name]

    def read_metadata_partitions_sequential(self, num_lines_per_chunk):
        pattern = re.compile(r"^>.*\n")
        partitions = []
        chunk_pos = 0
        with open(self.fasta_file, "rb") as f:
            i=0
            last_el = []
            titles = []
            while True:
                next_n_lines = list(islice(f, num_lines_per_chunk))
                chunk = b"".join(next_n_lines).decode("utf-8")
                match = re.finditer(r">.*\n",chunk)
                prev_chunk_pos = chunk_pos
                partition = []
                
                
                for e in list(match):
                    partition.append([e.start()+chunk_pos,e.end()+chunk_pos, "t"])
                    titles.append([e.start()+chunk_pos,e.end()+chunk_pos, "t"])
                 
                chunk_pos = chunk_pos + len(chunk)

                #Correct partitions for gem-indexer so it satisfies specifications.
                if partition != [] and partition[-1][1] != prev_chunk_pos :
                    partition.append([partition[-1][1],chunk_pos])

                if partition != [] and partition[0][0] != prev_chunk_pos :
                    partition.insert(0,[prev_chunk_pos,partition[0][0]])
                
                if partition != [] and len(partition[0])==2 and i > 0:   
                    partition.insert(0,titles[-2])              

                if partition != []:
                    partitions.append(partition)
                else:
                    partitions.append([titles[-1],[prev_chunk_pos,chunk_pos]])

               
                
                i+=1

                if not next_n_lines:
                    break
        return partitions
        
    

    
class FastqPartitionerDask():
    def __init__(self,fastq_file,npartitions):
        self.fastq_file = fastq_file
        self.npartitions = npartitions
       
    def _read_metadata_partitions(self,arr):
        return [' '.join(arr)]
    

class MapAlignment():
    def __init__(self):
        pass
    def _map_alignment_fasta(self,partition_product):
        print(partition_product)
        #cpus=multiprocessing.cpu_count()
        #return sp.run(['../gem3-mapper/bin/gem-indexer', '--input', partition_product[1], '--threads', str(cpus)], capture_output=True)

    

if __name__ == '__main__':
    
    #Set up the dask client
    c = Client(processes=False)

    #Class instances
    fapd = FastaPartitionerDask("fasta.fasta",2)
    fqpd = FastqPartitionerDask("fastq.fastq",4)
    map_alignment = MapAlignment()
    
    arr = fapd.read_metadata_partitions_sequential(10)
    #Partition fasta file using read_text and repartition (parallel)
    #fa = db.read_text(fapd.fasta_file).repartition(npartitions=fapd.npartitions).map_partitions(lambda x: fapd._read_metadata_partitions(x))
    
    
    #Partition fastq file
    #arr = fa.compute()

   
    #Partition fastq file based on number of partitions
    
    fq = db.read_text(fqpd.fastq_file).repartition(npartitions=fqpd.npartitions).map_partitions(lambda x: fqpd._read_metadata_partitions(x))

    for e in fq:
        for i in arr:
            c.submit(map_alignment._map_alignment_fasta, [e,i])
    
    #Map alignment
    #print(fq.product(fa).map(lambda cart_prod: map_alignment._map_alignment_fasta(cart_prod)).compute())

