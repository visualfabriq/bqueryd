import os
import logging
logging.basicConfig(format="%(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)
logger = logging.getLogger('bqueryd')

__version__ = 0.4
DEFAULT_DATA_DIR = '/srv/bcolz/'
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
READY = os.path.join(DEFAULT_DATA_DIR, 'ready')
for x in (INCOMING, READY):
    if not os.path.exists(x):
        os.mkdir(x)

REDIS_SET_KEY = 'bqueryd_controllers'

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode

