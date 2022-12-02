import boto3
import subprocess
import os
import math
import tempfile
import shutil
import argparse
import botocore

LOCAL_TMP_DIR = "fastq-data"


def create_fasta_splits(sra_ids, splits, bucket, prefix):
    s3 = boto3.client('s3')

    for sra_id in sra_ids:
        print(f'Creating dataset for SRA id {sra_id}')

        if not os.path.exists(os.path.join(LOCAL_TMP_DIR, sra_id + '.fastq')):
            print(f'Downloading FASTQ file {sra_id} from SRA...')
            cmd = ['fasterq-dump', '--concatenate-reads', '--skip-technical', '--outdir', LOCAL_TMP_DIR, sra_id]
            print(' '.join(cmd))
            subprocess.run(cmd, check=True)
        else:
            print(f'Found FASTQ file {sra_id}')

        file = os.path.join(LOCAL_TMP_DIR, sra_id+'.fastq')
        cmd = "wc -l "+ file +" | awk '{print $1;}'"
        print(cmd)
        lines = int(subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True).stdout)
        n_reads = math.ceil(lines / 4)
        print(f'{lines=},{n_reads=}')

        for split in range(1, splits + 1):
            if split == 1:
                key = os.path.join(prefix, sra_id + '_' + str(split), sra_id + '_0.fastq')
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                    exists = True
                except botocore.exceptions.ClientError:
                    exists = False
                if not exists:
                    print('Uploading single split...')
                    s3.upload_file(Filename=os.path.join(LOCAL_TMP_DIR, sra_id + '.fastq'), Bucket=bucket, Key=key)
                    print('Done')
                else:
                    print('Key already exists')
                continue

            lines_per_chunk = math.ceil(n_reads / split) * 4
            print(f'{lines_per_chunk=}')
            print(f'Going to split file in {split} chunks')
            tmp_dir = tempfile.mktemp()
            shutil.rmtree(tmp_dir, ignore_errors=True)
            os.mkdir(tmp_dir)
            cmd = ['split', '-l', str(lines_per_chunk), os.path.join(LOCAL_TMP_DIR, sra_id + '.fastq'), os.path.join(tmp_dir, 'chunk')]
            print(' '.join(cmd))
            subprocess.run(cmd, check=True)

            chunks = sorted(os.listdir(tmp_dir))
            print('Created chunks: ', chunks)

            for i, chunk in enumerate(chunks):
                key = os.path.join(prefix, sra_id + '_' + str(split), sra_id + '_' + str(i) + '.fastq')
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                    exists = True
                except botocore.exceptions.ClientError:
                    exists = False
                if not exists:
                    print(f'Uploading split {i}...')
                    s3.upload_file(Filename=os.path.join(tmp_dir, chunk), Bucket=bucket, Key=key)
                    print('Done')
                else:
                    print('Key alredy exists')
            
            shutil.rmtree(tmp_dir, ignore_errors=True)
            print(f'Done processing file {sra_id}')

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--bucket', required=True)
    args_parser.add_argument('--prefix', required=True)
    args_parser.add_argument('--input_file', type=str, required=False)
    args_parser.add_argument('--sra_id', type=str, required=False, action='append')
    args_parser.add_argument('--splits', default=5, type=int, required=False)
    args = args_parser.parse_args()
    print(args)

    sra_ids = []
    if args.sra_id is not None:
        sra_ids.extend(args.sra_id)
    if args.input_file is not None:
        with open(args.input_file, 'r') as input_file_file:
            sra_ids.extend(input_file_file.readlines())
    
    print(sra_ids)
    create_fasta_splits(sra_ids, args.splits, args.bucket, args.prefix)


