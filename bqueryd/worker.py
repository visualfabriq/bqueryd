import os
import time
import zmq
import bquery
import bcolz
import traceback
import cPickle
import logging
import redis
import binascii
from bqueryd.messages import msg_factory, WorkerRegisterMessage, ErrorMessage, BusyMessage, Message, StopMessage

DEFAULT_DATA_DIR = '/srv/bcolz/'
DATA_FILE_EXTENSION = '.bcolz'
DATA_SHARD_FILE_EXTENSION = '.bcolzs'
POLLING_TIMEOUT = 5000  # timeout in ms : how long to wait for network poll, this also affects frequency of seeing new controllers and datafiles
WRM_DELAY = 5 # how often in seconds to send a WorkerRegisterMessage
bcolz.set_nthreads(1)

logger = logging.getLogger('Worker')

class WorkerNode(object):

    def __init__(self, data_dir=DEFAULT_DATA_DIR, redis_url='redis://127.0.0.1:6379/0'):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception("Datadir %s is not a valid difrectory" % data_dir)
        self.worker_id = binascii.hexlify(os.urandom(8))
        self.data_dir = data_dir
        self.data_files = set()
        self.context = zmq.Context()
        self.redis_server = redis.from_url(redis_url)
        self.controllers = {} # Keep a dict of timestamps when you last spoke to controllers
        self.poller = zmq.Poller()
        self.check_controllers()
        self.last_wrm = time.time()


    def check_controllers(self):
        # Check the Redis set of controllers to see if any new ones have appeared,
        # Also register with them if so.
        listed_controllers = list(self.redis_server.smembers('bqueryd_controllers_sink'))
        current_controllers = []
        new_controllers = []
        for k,v in self.controllers.items():
            if v['address'] not in listed_controllers:
                del self.controllers[k]
                self.poller.unregister(k)
            else:
                current_controllers.append(v['address'])

        new_controllers = [c for c in listed_controllers if c not in current_controllers]

        for controller_address in new_controllers:
            controller = self.context.socket(zmq.DEALER)
            controller.setsockopt(zmq.LINGER, 0)
            controller.identity = self.worker_id
            controller.connect(controller_address)
            self.controllers[controller] = {'last_seen': 0, 'last_sent': 0, 'address': controller_address}
            self.poller.register(controller, zmq.POLLIN)

    def check_datafiles(self):
        has_new_files = False
        for data_file in [filename for filename in os.listdir(self.data_dir) if
                          filename.endswith(DATA_FILE_EXTENSION) or filename.endswith(DATA_SHARD_FILE_EXTENSION)]:
            if data_file not in self.data_files:
                self.data_files.add(data_file)
                has_new_files = True
        if len(self.data_files) < 1:
            logger.debug('Data directory %s has no files like %s or %s' % (
                self.data_dir, DATA_FILE_EXTENSION, DATA_SHARD_FILE_EXTENSION))
        return has_new_files


    def prepare_wrm(self):
        wrm = WorkerRegisterMessage()
        wrm['worker_id'] = self.worker_id
        wrm['data_files'] = list(self.data_files)
        wrm['data_dir'] = self.data_dir
        wrm['controllers'] = self.controllers.values()
        return wrm


    def heartbeat(self):
        since_last_wrm = time.time() - self.last_wrm
        if since_last_wrm > WRM_DELAY:
            self.check_controllers()
            has_new_files = self.check_datafiles()
            self.last_wrm = time.time()
            wrm = self.prepare_wrm()
            for controller, data in self.controllers.items():
                if has_new_files or (time.time() - data['last_seen'] > WRM_DELAY):
                    controller.send_json(wrm)
                    data['last_sent'] = time.time()
                    logger.debug("register to %s" % data['address'])


    def go(self):

        self.running = True
        while self.running:

            self.heartbeat()

            for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                if event & zmq.POLLIN:
                    data = self.controllers[sock]
                    data['last_seen'] = time.time()
                    msg = sock.recv_json()
                    msg = msg_factory(msg)
                    logger.debug('%s received from %s' % (self.worker_id, data['address']))
                    # TODO Notify Controllers that we are busy, no more messages to be sent
                    self.send_to_all_except_own(sock, BusyMessage())
                    # The above busy notification is not perfect as other messages might be on their way already
                    # but for long-running queries it will at least ensure other controllers
                    # don't try and overuse this node by filling up a queue
                    try:
                        tmp = self.handle(msg)
                    except Exception, e:
                        tmp = ErrorMessage(msg)
                        tmp['payload'] = traceback.format_exc()
                    sock.send_json(tmp)
                    self.send_to_all_except_own(sock, Message()) # Send an empty mesage to all controllers, this flags you as 'Done'
        logger.debug('Stopping %s' % self.worker_id)

    def send_to_all_except_own(self, sock, msg):
        for controller in self.controllers:
            if not controller is sock:
                controller.send_json(msg)

    def handle(self, msg):
        buf = '' # placeholder results buffer
        params = msg.get('params', {})
        if params:
            tmp = params.decode('base64')
            params = cPickle.loads(tmp)
        kwargs = params.get('kwargs', {})
        args = params.get('args', [])

        if msg.get('payload') == 'kill':
            # Also send a message to all your controllers, that you are stopping
            for controller in self.controllers:
                controller.send_json(StopMessage())
            self.running = False
            return
        elif msg.get('payload') == 'info':
            msg = self.prepare_wrm()
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