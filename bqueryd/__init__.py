import logging
logging.basicConfig(format="%(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)

__version__ = 0.3

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode