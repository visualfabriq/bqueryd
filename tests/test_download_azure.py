import logging
import os
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import redis
import shutil
import socket
import threading
import time
from bcolz import ctable
from time import sleep
from uuid import uuid4

import bqueryd

from ..worker import DownloaderNode

from azure.storage.blob.blob_client import BlobClient, BlobServiceClient

TEST_REDIS = 'redis://192.32.1.5:6379/0'
ACCOUNT_URL = 'https://azurestorageepos.blob.core.windows.net/'
AZURE_CONN_STRING = 'DefaultEndpointsProtocol=https;AccountName=azurestorageepos;AccountKey=6GZYZp5CscDriXst8EaRUUa96'\
                    '+2Xzr5ooqYGOdiej8EsHL9ZdEXcSbLf0G2YdoUtdfkNAwtPx9KwcG5ucA2TGQ==;EndpointSuffix=core.windows.net'

@pytest.fixture
def redis_server():
    """
    Return a Redis Client connected to local (test) redis.
    Remove all keys from REDIS before and after use.
    """
    redis_server = redis.from_url(TEST_REDIS)
    redis_server.flushdb()
    yield redis_server
    redis_server.flushdb()


@pytest.fixture
def clear_incoming():
    if os.path.isdir(bqueryd.INCOMING):
        shutil.rmtree(bqueryd.INCOMING)


@pytest.fixture
def downloader():
    downloader = DownloaderNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG,
                                        azure_conn_string=AZURE_CONN_STRING)
    downloader_thread = threading.Thread(target=downloader.go)
    downloader_thread.daemon = True
    downloader_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    yield downloader

    # shutdown the downloader
    downloader.running = False


@contextmanager
def clean_container():
    blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL)
    blob_service_client.create_container('test')

    yield blob_service_client

    blob_service_client.delete_container('test')


@pytest.mark.usefixtures('clear_incoming')
def test_downloader(redis_server, downloader, tmpdir):
    # Make a bcolz from a pandas DataFrame
    data_df = pd.DataFrame(
        data=np.random.rand(100, 10),
        columns=['col_{}'.format(i + 1) for i in range(10)])
    local_bcolz = str(tmpdir.join('test_bcolz'))
    ctable.fromdataframe(data_df, rootdir=local_bcolz)

    assert os.path.isdir(local_bcolz)

    # Zip up the bcolz directory
    upload_dir = tmpdir.mkdir('upload')
    zipfile_path = bqueryd.util.zip_to_file(local_bcolz, str(upload_dir))[0]
    assert os.path.isfile(zipfile_path)

    upload_file = str(upload_dir.join('test.bcolz'))
    shutil.move(zipfile_path, upload_file)
    assert os.path.isfile(upload_file)

    # Upload to azure

    with clean_container() as container:
        blob_client = BlobClient(ACCOUNT_URL, container='test')
        blob_client.upload_blob(open(upload_file, 'rb'))

        stream = blob_client.download_blob()
        assert stream is not None

        # Construct the redis entry that the downloader is looking for
        progress_slot = '%s_%s' % (time.time() - 60, -1)
        node_filename_slot = '%s_%s' % (socket.gethostname(), 'azure://bcolz/test.bcolz')
        ticket = str(uuid4())

        incoming_dir = os.path.join(bqueryd.INCOMING, ticket)
        assert not os.path.isdir(incoming_dir)

        redis_server.hset(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

        # wait for the downloader to catch up
        sleep(10)

        # Check that incoming dir now has the test.bcolz file.
        assert os.listdir(incoming_dir) == ['test.bcolz']

        # Check that the progress slot has been updated
        updated_slot = redis_server.hget(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        assert updated_slot.split('_')[-1] == 'DONE'


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
