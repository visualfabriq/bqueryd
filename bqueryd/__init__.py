__version__ = 0.9
import os
import logging
logger = logging.getLogger('bqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

DEFAULT_DATA_DIR = '/srv/bcolz/'
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    os.makedirs(INCOMING)

REDIS_SET_KEY = 'bqueryd_controllers'

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode

