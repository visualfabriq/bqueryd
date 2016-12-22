import logging
logging.basicConfig(format="%(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)

__version__ = 0.1

VENTILATOR_PORT = 14335
RPC_PORT = 14337

from rpc import RPC
from controller import ControllerNode
from worker import WorkerNode

def threaded():
    import threading

    w = WorkerNode()
    worker = threading.Thread(target=w.go)
    q = ControllerNode()
    queue = threading.Thread(target=q.go)

    worker.start()
    queue.start()