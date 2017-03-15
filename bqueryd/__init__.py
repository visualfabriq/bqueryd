__version__ = 0.9
import os
import logging
logger = logging.getLogger('bqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

DEFAULT_DATA_DIR = '/srv/bcolz/'
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    os.makedirs(INCOMING)

REDIS_SET_KEY = 'bqueryd_controllers'
REDIS_TICKET_KEY_PREFIX = 'bqueryd_download_ticket_'
REDIS_DOWNLOAD_LOCK_PREFIX = 'bqueryd_download_lock_'
REDIS_DOWNLOAD_LOCK_DURATION = 60 * 30 # time in seconds to keep a lock
REDIS_DOWNLOAD_LOCK_MOVE_DURATION = 60 * 30 # time in seconds to keep a lock
# TODO dynamic nature of DQEng failing now due to out-of-memory errors. hard-code this and revisit soon to make it more dynamic again
NODES = ['dqe11', 'dqe12', 'dqe13', 'dqe14', 'dqe15']

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode, DownloaderNode

