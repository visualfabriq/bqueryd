import os, threading
from bqueryd import *

for x in range(3):
    w = WorkerNode()
    worker = threading.Thread(target=w.go)
    worker.start()

q = QNode()
queue = threading.Thread(target=q.go)


queue.start()

open('test.pid', 'w').write(str(os.getpid())) # convenience to kill the whole shebang with:  kill `cat test.pid` :-)

a = RPC()
print a.info()
print a.groupby(['payment_type'], ['extra'], _filename='taxi_0/')
a.kill()
