import logging
import os
import socket

import bcolz
import pandas as pd
import pytest
import redis
import threading
from bcolz import ctable
from pandas.util.testing import assert_frame_equal
from time import sleep

import bqueryd
from bqueryd.util import get_my_ip

TEST_REDIS = 'redis://redis:6379/0'
NR_SHARDS = 5


@pytest.fixture(scope='module')
def taxi_df():
    taxi_df = pd.read_csv(
        '/srv/datasets/yellow_tripdata_2016-01.csv',
        parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    )
    yield taxi_df


@pytest.fixture(scope='module')
def bcolz_dir(request, tmpdir_factory):
    """A tmpdir fixture for the module scope. Persists throughout the module."""
    return tmpdir_factory.mktemp(request.module.__name__)


class DummyDownloader(bqueryd.DownloaderNode):

    def download_file(self, ticket, filename):
        self.file_downloader_progress(ticket, filename, 'DONE')


@pytest.fixture(scope='module')
def rpc(bcolz_dir):
    redis_server = redis.from_url(TEST_REDIS)
    redis_server.flushdb()
    controller = bqueryd.ControllerNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    controller_thread = threading.Thread(target=controller.go)
    controller_thread.daemon = True
    controller_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    worker = bqueryd.WorkerNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG, data_dir=str(bcolz_dir),
                                restart_check=False)
    worker_thread = threading.Thread(target=worker.go)
    worker_thread.daemon = True
    worker_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    downloader = DummyDownloader(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    downloader_thread = threading.Thread(target=downloader.go)
    downloader_thread.daemon = True
    downloader_thread.start()
    sleep(5)

    rpc = bqueryd.RPC(timeout=100, redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    yield rpc

    # shutdown the controller and worker
    controller.running = False
    worker.running = False
    downloader.running = False


@pytest.fixture(scope='module')
def shards(bcolz_dir, taxi_df):
    single_bcolz = str(bcolz_dir.join('yellow_tripdata_2016-01.bcolz'))
    ct = ctable.fromdataframe(taxi_df, rootdir=single_bcolz)

    step, remainder = divmod(len(ct), NR_SHARDS)
    count = 0
    shards = [single_bcolz]

    for idx in range(0, len(ct), step):
        print("Creating shard {}".format(count+1))

        if idx == len(ct) * (NR_SHARDS - 1):
            step = step + remainder

        shard_file = str(bcolz_dir.join('tripdata_2016-01-%s.bcolzs' % count))
        ct_shard = bcolz.fromiter(
            ct.iter(idx, idx + step),
            ct.dtype,
            step,
            rootdir=shard_file,
            mode='w'
        )
        shards.append(shard_file)

        ct_shard.flush()
        count += 1

    yield shards


def test_rpc_info(rpc):
    result = rpc.info()

    address = result['address']
    my_ip = get_my_ip()
    assert my_ip == address.split(':')[1].replace('//', '')

    workers = result['workers']
    assert len(workers) == 2
    worker_node = list(v for v in workers.values() if v['workertype'] == 'calc')[0]

    node = result['node']
    assert node == worker_node['node']

    downloader_node = list(v for v in workers.values() if v['workertype'] == 'download')[0]
    assert node == downloader_node['node']

    assert result['others'] == {}


def test_download(rpc):
    ticket_nr = rpc.download(filenames=['test_download.bcolz'], bucket='bcolz', wait=False)
    redis_server = redis.from_url(TEST_REDIS)
    download_entries = redis_server.hgetall(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket_nr)
    assert len(download_entries) == 1
    for key, value in download_entries.items():
        # filename
        assert '_'.join(key.split('_')[1:]) == "s3://bcolz/test_download.bcolz"
        # progress slot
        assert value.split('_')[-1] == "-1"


def test_compare_with_pandas_total_amount_sum(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'total_amount', 'sum')


def test_compare_with_pandas_total_amount_mean(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'total_amount', 'mean')


def test_compare_with_pandas_payment_type_count(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'passenger_count', 'count')


def compare_with_pandas(taxi_df, rpc, shards, group_col, agg_col, method):
    full = os.path.basename(shards[0])
    full_result = rpc.groupby([full], [group_col], [[agg_col, method, agg_col]], [])
    full_result = full_result.sort_values(by=group_col)

    gp = taxi_df.groupby(group_col, sort=True)[agg_col]
    if method == 'sum':
        pandas_result = gp.sum()
    elif method == 'mean':
        pandas_result = gp.mean()
    elif method == 'count':
        pandas_result = gp.count()
    else:
        assert False, "Unknown method: {}".format(method)

    pandas_result = pandas_result.reset_index()

    # Make indexes same since we don't care about comparing them
    full_result.index = pd.Index(data=range(0, full_result.shape[0]))
    pandas_result.index = pd.Index(data=range(0, pandas_result.shape[0]))

    assert_frame_equal(full_result, pandas_result, check_less_precise=True)


def test_compare_full_with_shard(rpc, shards):
    shard_filenames = [os.path.basename(x) for x in shards]

    full, parts = shard_filenames[:1], shard_filenames[1:]
    full_result = rpc.groupby(full, ['payment_type'], [['passenger_count', 'count', 'passenger_count']], [])
    parts_result = rpc.groupby(parts, ['payment_type'], [['passenger_count', 'count', 'passenger_count']], [])

    assert isinstance(full_result, pd.DataFrame)
    assert isinstance(parts_result, pd.DataFrame)

    # This returns a single DataFrame with the results from each part pasted in it separately
    # so we need to use a further groupby to produce the same result as with the full bcolz
    parts_result = parts_result.groupby('payment_type', sort=True).sum()
    full_result = full_result.set_index('payment_type').sort_index()

    assert_frame_equal(full_result, parts_result)


if __name__ == '__main__':
    pytest.main([__file__, '-s'])
