import logging
logging.basicConfig(format="%(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)
logger = logging.getLogger('bqueryd')

__version__ = 0.4
DEFAULT_DATA_DIR = '/srv/bcolz/'
REDIS_SET_KEY = 'bqueryd_controllers'

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode
from filemanager import FileManagerNode

