import pandas as pd
import boto3
import tempfile
import subprocess
import time
import os


BUCKET = 'lithops-datasets'
FASTA_PREFIX = 'fastq/'
SAVE_COMPRESSED = True
FASTQGZ_PREFIX = 'fastq.gz/'
STATS_FILE = 'fastqgz_compression_overhead.csv'


def load_stats():
    if os.path.exists(STATS_FILE):
        df = pd.read_csv(STATS_FILE)
        stats = df.to_dict()
        return stats
    else:
        return []


def save_stats(stats):
    df = pd.DataFrame.from_dict(stats)
    df.to_csv('fastqgz_compression_overhead.csv', index=False)


if __name__ == '__main__':
    stats = load_stats()
    s3 = boto3.client('s3')

    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=FASTA_PREFIX)
    keys = [obj['Key'] for obj in res['Contents']]

    for key in keys:
        if key in stats:
            print(f'{key} already processed, skipping...')
            continue

        tmp_fasta_file = tempfile.mktemp() + '.fasta'
        if os.path.exists(tmp_fasta_file):
            os.remove(tmp_fasta_file)

        print(f'Downloading {key}...')
        s3.download_file(Bucket=BUCKET, Key=key, Filename=tmp_fasta_file)

        fasta_sz = os.stat(tmp_fasta_file).st_size

        cmd = ['gzip', '-6', '-k', tmp_fasta_file]
        print(' '.join(cmd))
        t0 = time.perf_counter()
        subprocess.run(cmd, check=True)
        t1 = time.perf_counter()

        if SAVE_COMPRESSED:
            print('Uploading compressed fasta...')
            s3.upload_file(Filename=tmp_fasta_file + '.gz',
                           Bucket=BUCKET, Key=key.replace(FASTA_PREFIX, FASTQGZ_PREFIX) + '.gz')

        gz_time = t1 - t0
        fastagz_sz = os.stat(tmp_fasta_file + '.gz').st_size
        os.remove(tmp_fasta_file + '.gz')

        tmp_fasta_index_file = tempfile.mktemp() + '.gzi'
        if os.path.exists(tmp_fasta_index_file):
            os.remove(tmp_fasta_index_file)

        cmd = ['gztool', '-c', '-6', '-x', '-I', tmp_fasta_index_file, tmp_fasta_file]
        print(' '.join(cmd))
        t0 = time.perf_counter()
        subprocess.run(cmd, check=True)
        t1 = time.perf_counter()

        gzi_time = t1 - t0
        fastagzi_sz = os.stat(tmp_fasta_index_file).st_size
        os.remove(tmp_fasta_file + '.gz')
        os.remove(tmp_fasta_index_file)

        stat = {'key': key, 'fasta_size': fasta_sz,
                'fasta_gzip_time': gz_time, 'fasta_gzip_size': fastagz_sz,
                'fasta_gzindex_time': gzi_time, 'fasta_gzindex_size': fastagzi_sz}
        print(stat)
        stats[key] = stat
        save_stats(stats)
