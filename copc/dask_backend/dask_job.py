import time
import itertools

from dask.distributed import Client, get_client, as_completed
from pprint import pprint
import dask.bytes
import dask.bag

from lpc_workflow import partition_las, create_dem, merge_dem_partitions


def partition_las_dask_wrapper(args):
    chunk, path = args
    data = chunk[0].compute()
    partitions = partition_las(file_path=path, lidar_data=data)
    return partitions


def create_dem_dask_wrapper(args):
    file_path, partition, las_data = args
    models = create_dem(file_path, partition, las_data)
    return models


def merge_partitions_dask_wrapper(args):
    key, partitions = args
    result = merge_dem_partitions(key, partitions)
    return key


def partition_las_dask_futures_wrapper(args):
    chunk, path = args
    lidar_data = chunk[0].compute()
    partitions = partition_las(file_path=path, lidar_data=lidar_data)
    client = get_client()
    part_futures = client.scatter(partitions)
    return part_futures


def create_dem_dask_futures_wrapper(args):
    file_path, partition, las_data = args
    # las_data = las_data_future.result()
    file_path, partition, dem = create_dem(file_path, partition, las_data)
    client = get_client()
    dem_future = client.scatter(dem)
    return file_path, partition, dem_future


def merge_partitions_dask_futures_wrapper(args):
    key = args[0][0]
    result = merge_dem_partitions(key, args)
    return key


def run_naive_workflow():
    storage_options = {
        'key': 'minioadmin',
        'secret': 'minioadmin',
        'client_kwargs': {
            'region_name': 'us-east-1',
            'endpoint_url': 'http://192.168.1.110:9000'
        }
    }

    client = Client(threads_per_worker=1, n_workers=4, processes=True)
    client.start()

    _, blocks, paths = dask.bytes.read_bytes('s3://geospatial/las/*.las', include_path=True, **storage_options)

    bag = dask.bag.from_sequence(zip(blocks, paths))

    pipeline = (bag
                .map(partition_las_dask_wrapper)
                .flatten()
                .map(create_dem_dask_wrapper)
                .groupby(lambda args: args[0])
                .map(merge_partitions_dask_wrapper))

    client.compute(pipeline, sync=True)


def run_futures_naive_workflow():
    storage_options = {
        'key': 'minioadmin',
        'secret': 'minioadmin',
        'client_kwargs': {
            'region_name': 'us-east-1',
            'endpoint_url': 'http://192.168.1.110:9000'
        }
    }

    client = Client(threads_per_worker=1, n_workers=4, processes=True)
    client.start()

    _, blocks, paths = dask.bytes.read_bytes('s3://geospatial/laz/cnig/*.laz', include_path=True, **storage_options)

    iterdata = list(zip(blocks, paths))
    partition_futures = client.map(partition_las_dask_futures_wrapper, iterdata)

    models_futures = []
    for fut in as_completed(partition_futures):
        part_futs = fut.result()
        for part_fut in part_futs:
            model_future = client.submit(create_dem_dask_futures_wrapper, part_fut)
            models_futures.append(model_future)

    dems = client.gather(models_futures)
    grouped_dems = []
    for key, group in itertools.groupby(dems, lambda part: part[0]):
        grouped_dems.append(list(group))

    merge_futures = client.map(merge_partitions_dask_futures_wrapper, grouped_dems)
    merged = client.gather(merge_futures)

    print(merged)


if __name__ == '__main__':
    # run_naive_workflow()
    run_futures_naive_workflow()
