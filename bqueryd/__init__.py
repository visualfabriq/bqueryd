__version__ = 0.6
import os
import logging
logging.basicConfig(format="%(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)
logger = logging.getLogger('bqueryd')
b = logging.getLogger('boto')
b.setLevel(logging.INFO)

DEFAULT_DATA_DIR = '/srv/bcolz/'
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    os.mkdir(INCOMING)

REDIS_SET_KEY = 'bqueryd_controllers'

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode

