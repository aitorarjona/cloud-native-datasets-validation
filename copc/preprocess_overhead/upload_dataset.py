import boto3
import botocore.exceptions
import requests
import os
import urllib3
import concurrent.futures

BUCKET = 'lithops-datasets'
PREFIX = 'copc-preprocess-overhead/CA_YosemiteNP_2019'
WORKERS = 4
LINKS_FILENAME = 'dataset_links.txt'

if __name__ == '__main__':
    def download_and_upload(link):
        filename = os.path.basename(link)
        key = os.path.join(PREFIX, filename)
        s3 = boto3.client('s3')

        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            exists = True
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == '404':
                exists = False
            else:
                raise error

        if exists:
            print(f'Key {key} exists')
            return

        head_res = requests.head(link)
        print(f'Uploading {filename} (size {head_res.headers["Content-Length"]})')
        response = http.request('GET', link, preload_content=False)
        s3.upload_fileobj(Bucket=BUCKET, Key=key, Fileobj=response)
        print(f'Put {key} OK')

    http = urllib3.PoolManager()

    with open(LINKS_FILENAME, 'r') as file:
        links = file.readlines()

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for link in links:
            f = pool.submit(download_and_upload, link)
            futures.append(f)
        [f.result() for f in futures]
