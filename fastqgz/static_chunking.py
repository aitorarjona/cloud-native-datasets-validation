import boto3
import botocore
from concurrent.futures import ThreadPoolExecutor
import gzip

N_LINES = 6709944
CHUNKS = 256
LINES_CHUNK = round(N_LINES / CHUNKS)


def _async_uploader(buff, chunk_id):
    res = s3_client.put_object(Body=buff, Bucket='genomics', Key=f'chunktest/{chunk_id}.txt')
    print(res)



if __name__ == '__main__':
    s3_client = boto3.client('s3',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',
        endpoint_url='http://192.168.1.110:9000',
        config=botocore.client.Config(signature_version='s3v4')
    )

    data_stream = s3_client.get_object(Bucket='genomics', Key='SRR21394969.fastq.gz')

    with ThreadPoolExecutor(max_workers=256) as pool:
        futures = []
        zip_stream = gzip.GzipFile(fileobj=data_stream['Body'])
        for chunk_n in range(CHUNKS):
            print(chunk_n)
            lines =  zip_stream.readlines(LINES_CHUNK)
            buff = b''.join(lines)
            f = pool.submit(_async_uploader, buff, chunk_n)
            futures.append(f)
        [f.result() for f in futures]
