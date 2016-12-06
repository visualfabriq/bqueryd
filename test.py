import os, threading
from bqueryd import *

w = WorkerNode()
worker = threading.Thread(target=w.go)

q = QNode()
queue = threading.Thread(target=q.go)

worker.start()
queue.start()

open('test.pid', 'w').write(str(os.getpid()))