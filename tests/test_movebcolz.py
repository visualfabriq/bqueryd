import logging
import os

import numpy as np
import pandas as pd
import pytest
import redis
import shutil
import socket
import threading
import time
from bquery import ctable
from time import sleep
from uuid import uuid4

import bqueryd

TEST_REDIS = 'redis://redis:6379/0'


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
def clear_dirs():
    if os.path.isdir(bqueryd.INCOMING):
        shutil.rmtree(bqueryd.INCOMING)
    if os.path.isdir(bqueryd.DEFAULT_DATA_DIR):
        shutil.rmtree(bqueryd.DEFAULT_DATA_DIR)


@pytest.fixture
def mover():
    mover = bqueryd.MoveBcolzNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    mover_thread = threading.Thread(target=mover.go)
    mover_thread.daemon = True
    mover_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    yield mover

    # shutdown the thread
    mover.running = False


@pytest.mark.usefixtures('clear_dirs')
@pytest.mark.usefixtures('mover')
def test_movebcolz(redis_server, tmpdir):
    # Make a bcolz from a pandas DataFrame
    data_df = pd.DataFrame(
        data=np.random.rand(100, 10),
        columns=['col_{}'.format(i+1) for i in range(10)])
    local_bcolz = str(tmpdir.join('test_mover.bcolz'))
    ctable.fromdataframe(data_df, rootdir=local_bcolz)

    assert os.path.isdir(local_bcolz)

    # copy the bcolz directory to bqueyd.INCOMING
    ticket = str(uuid4())
    ticket_dir = os.path.join(bqueryd.INCOMING, ticket, 'test_mover.bcolz')
    shutil.copytree(local_bcolz, ticket_dir)

    # Construct the redis entry that before downloading
    progress_slot = '%s_%s' % (time.time() - 60, -1)
    node_filename_slot = '%s_%s' % (socket.gethostname(), 's3://bcolz/test_mover.bcolz')

    redis_server.hset(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

    # wait for some time
    sleep(5)

    # At this stage, we don't expect the bcolz directory to be moved to bqueryd.DEFAULT_DATA_DIR, because the progress
    # slot has not been updated yet
    files_in_default_data_dir = os.listdir(bqueryd.DEFAULT_DATA_DIR)
    files_in_default_data_dir.sort()
    assert files_in_default_data_dir == ['incoming']
    # ticket_dir still exists
    assert os.path.isdir(ticket_dir)

    # Now update progress slot
    new_progress_slot = '%s_%s' % (time.time(), 'DONE')
    redis_server.hset(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, new_progress_slot)

    # Sleep again
    sleep(5)
    files_in_default_data_dir = os.listdir(bqueryd.DEFAULT_DATA_DIR)
    files_in_default_data_dir.sort()
    assert files_in_default_data_dir == ['incoming', 'test_mover.bcolz']
    # ticket_dir should have been deleted.
    assert not os.path.exists(ticket_dir)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])