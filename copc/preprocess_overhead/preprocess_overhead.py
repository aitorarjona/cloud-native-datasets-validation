import logging

import boto3
import os
import time
import json

from dataplug import CloudObject
from dataplug.geospatial.laspc import LiDARPointCloud
from dataplug.preprocess import LithopsPreprocessor, DummyPreprocessor
from dataplug.util import setup_logging

from datetime import datetime


BUCKET = 'lithops-datasets'
PREFIX = 'copc-preprocess-overhead/CA_YosemiteNP_2019/'

RESULTS_FILE = 'results.json'


def main():
    setup_logging(logging.DEBUG)

    s3 = boto3.client('s3')
    objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    keys = [obj['Key'] for obj in objs['Contents']]

    for key in keys:

        co = CloudObject.from_s3(LiDARPointCloud,
                                 f's3://{BUCKET}/{key}')

        preprocessed = co.is_preprocessed()
        if not preprocessed:
            print(f'Going to preproces {key}...')
            backend = DummyPreprocessor()
            t0 = time.perf_counter()
            co.preprocess(backend)
            t1 = time.perf_counter()
            print(f'Took {round(t1-t0, 3)} s')


        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'r') as f:
                results = json.loads(f.read())
        else:
            results = []

        results.append({'key': key, 'size': co.size, 'preprocess_time': t1 - t0})

        with open(RESULTS_FILE, 'w') as f:
            f.write(json.dumps(results))


if __name__ == '__main__':
    main()
