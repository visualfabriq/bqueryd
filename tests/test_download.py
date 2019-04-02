import logging
import os
from contextlib import contextmanager

import boto3
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

TEST_REDIS = 'redis://redis:6379/0'
FAKE_ACCESS_KEY = 'fake access key'
FAKE_SECRET_KEY = 'fake secret key'


class LocalS3Downloader(bqueryd.DownloaderNode):
    """Extend ``bqueryd.DownloaderNode`` to return an S3 Resource connected to local (test) S3."""

    def __init__(self, *args, **kwargs):
        super(LocalS3Downloader, self).__init__(*args, **kwargs)

        session = boto3.session.Session(
            aws_access_key_id=FAKE_ACCESS_KEY,
            aws_secret_access_key=FAKE_SECRET_KEY,
            region_name='eu-west-1')

        self._conn = session.resource('s3', endpoint_url='http://localstack:4572')

    def _get_s3_conn(self):
        return FAKE_ACCESS_KEY, FAKE_SECRET_KEY, self._conn

    def _get_transport_params(self):
        return {'endpoint_url': 'http://localstack:4572'}


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
    downloader = LocalS3Downloader(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    downloader_thread = threading.Thread(target=downloader.go)
    downloader_thread.daemon = True
    downloader_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    yield downloader

    # shutdown the downloader
    downloader.running = False


@contextmanager
def clean_bucket(s3_conn, bucket_name):
    bucket = s3_conn.create_bucket(Bucket=bucket_name)
    for item in bucket.objects.all():
        item.delete()

    yield bucket

    for item in bucket.objects.all():
        item.delete()

    bucket.delete()


@pytest.mark.usefixtures('clear_incoming')
def test_downloader(redis_server, downloader, tmpdir):
    # Make a bcolz from a pandas DataFrame
    data_df = pd.DataFrame(
        data=np.random.rand(100, 10),
        columns=['col_{}'.format(i+1) for i in range(10)])
    local_bcolz = str(tmpdir.join('test_bcolz'))
    ctable.fromdataframe(data_df, rootdir=local_bcolz)

    assert os.path.isdir(local_bcolz)

    # Zip up the bcolz directory and upload to S3
    upload_dir = tmpdir.mkdir('upload')
    zipfile_path = bqueryd.util.zip_to_file(local_bcolz, str(upload_dir))[0]
    assert os.path.isfile(zipfile_path)

    upload_file = str(upload_dir.join('test.bcolz'))
    shutil.move(zipfile_path, upload_file)
    assert os.path.isfile(upload_file)

    s3_conn = downloader._get_s3_conn()[-1]

    with clean_bucket(s3_conn, 'bcolz') as bucket:
        bucket.put_object(Key='test.bcolz', Body=open(upload_file, 'rb'))

        uploads = [key.key for key in bucket.objects.all()]
        assert uploads == ['test.bcolz']

        # Construct the redis entry that the downloader is looking for
        progress_slot = '%s_%s' % (time.time() - 60, -1)
        node_filename_slot = '%s_%s' % (socket.gethostname(), 's3://bcolz/test.bcolz')
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
