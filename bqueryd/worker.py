import os
import time
import zmq
import bquery
import bcolz
import traceback
import cPickle
import logging
import redis
import bqueryd
from bqueryd.messages import msg_factory, WorkerRegisterMessage, ErrorMessage

DEFAULT_DATA_DIR = '/srv/bcolz/'
DATA_FILE_EXTENSION = '.bcolz'
DATA_SHARD_FILE_EXTENSION = '.bcolzs'
bcolz.set_nthreads(1)

logger = logging.getLogger('Worker')

class WorkerNode(object):

    def __init__(self, data_dir=DEFAULT_DATA_DIR, redis_url='redis://127.0.0.1:6379/0'):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception("Datadir %s is not a valid difrectory" % data_dir)
        self.data_dir = data_dir
        self.data_files = [filename for filename in os.listdir(self.data_dir) if
                           filename.endswith(DATA_FILE_EXTENSION) or filename.endswith(DATA_SHARD_FILE_EXTENSION)]
        if len(self.data_files) < 1:
            logger.debug('Data directory %s has no files like %s or %s' % (
                self.data_dir, DATA_FILE_EXTENSION, DATA_SHARD_FILE_EXTENSION))

        # Check the redis set of Controllers, and connect to all of them
        redis_server = redis.from_url(redis_url)
        controllers = redis_server.smembers('bqueryd_controllers_sink')
        if not controllers:
            raise Exception('No Controllers found in Redis set: bqueryd_controllers_sink')

        self.context = zmq.Context()
        wrm = WorkerRegisterMessage()
        self.worker_id = wrm['worker_id']
        wrm['data_files'] = self.data_files
        wrm['data_dir'] = self.data_dir
        self.controllers = []

        for controller_address in controllers:
            # We receive operations to perform from the ventilator on the controller socket
            controller = self.context.socket(zmq.DEALER)
            controller.identity = self.worker_id
            controller.connect(controller_address)
            controller.send_json(wrm)
            self.controllers.append(controller)


    def go(self):
        poller = zmq.Poller()
        for controller in self.controllers:
            poller.register(controller, zmq.POLLIN)

        self.running = True
        while self.running:
            time.sleep(0.0001)  # give the system a breather to stop CPU usage being pegged at 100%

            for sock, event in poller.poll():
                if event & zmq.POLLIN:
                    msg = sock.recv_json()
                    msg = msg_factory(msg)
                    logger.debug('%s received msg %s' % (self.worker_id, msg.get('token', '?')))
                    # TODO Notify Controllers that we are busy, no more messages to be sent
                    # The above busy notification is not perfect as other messages might be on their way already
                    # but for long-running queries it will at least ensure other controllers
                    # don't try and overuse this node by filling up a queue
                    try:
                        tmp = self.handle(msg)
                    except Exception, e:
                        tmp = ErrorMessage(msg)
                        tmp['payload'] = traceback.format_exc()
                    sock.send_json(tmp)
                    # TODO send a periodic heartbeat to the controller, in case of transience
        logger.debug('Stopping %s' % self.worker_id)

    def handle(self, msg):
        params = msg.get('params', {})
        if params:
            tmp = params.decode('base64')
            params = cPickle.loads(tmp)
        kwargs = params.get('kwargs', {})
        args = params.get('args', [])

        if msg.get('payload') == 'kill':
            self.running = False
            return
        elif msg.get('payload') == 'sleep':
            time.sleep(float(args[0]))
            buf = 'zzzzz'
        else:
            filename = args[0]
            groupby_col_list = args[1]
            aggregation_list = args[2]
            where_terms_list = args[3]
            expand_filter_column = kwargs.get('expand_filter_column')
            aggregate = kwargs.get('aggregate', True)

            # create rootdir
            rootdir = os.path.join(self.data_dir, filename)
            if not os.path.exists(rootdir):
                msg['payload'] = 'Path %s does not exist' % rootdir
                return ErrorMessage(msg)

            ct = bquery.ctable(rootdir=rootdir, mode='r')
            ct.auto_cache = False

            # prepare filter
            if not where_terms_list:
                bool_arr = None
            else:
                bool_arr = ct.where_terms(where_terms_list)

            # expand filter column check
            if expand_filter_column:
                bool_arr = ct.is_in_ordered_subgroups(basket_col=expand_filter_column, bool_arr=bool_arr)

            # retrieve & aggregate if needed
            if aggregate:
                # aggregate by groupby parameters
                result_ctable = ct.groupby(groupby_col_list, aggregation_list, bool_arr=bool_arr)
                buf = result_ctable.todataframe()
            else:
                # direct result from the ctable
                column_list = groupby_col_list + [x[0] for x in aggregation_list]
                if bool_arr is not None:
                    ct = bcolz.fromiter(ct[column_list].where(bool_arr), ct[column_list].dtype, sum(bool_arr))
                else:
                    ct = bcolz.fromiter(ct[column_list], ct[column_list].dtype, ct.len)
                buf = ct[column_list].todataframe()

        msg.add_as_binary('result', buf)

        return msg