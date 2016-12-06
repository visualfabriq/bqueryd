import os, threading
from bqueryd import *

w = WorkerNode()
worker = threading.Thread(target=w.go)

q = QNode()
queue = threading.Thread(target=q.go)

worker.start()
queue.start()

open('test.pid', 'w').write(str(os.getpid())) # convenience to kill the whole shebang with:  kill `cat test.pid` :-)

a = RPC()
print a.info()

result = a.calc(filename='taxi', groupby_cols=['payment_type'], agg_list=['nr_rides'])
