import os
import time
import zmq
import bquery
import bcolz
import traceback
import tempfile
import zipfile
import shutil
import boto
import redis
import binascii
import logging
import random
import bqueryd
import socket
from bqueryd.messages import msg_factory, WorkerRegisterMessage, ErrorMessage, BusyMessage, StopMessage, \
    DoneMessage, FileDownloadProgress

DATA_FILE_EXTENSION = '.bcolz'
DATA_SHARD_FILE_EXTENSION = '.bcolzs'
POLLING_TIMEOUT = 5000  # timeout in ms : how long to wait for network poll, this also affects frequency of seeing new controllers and datafiles
WRM_DELAY = 20 # how often in seconds to send a WorkerRegisterMessage
bcolz.set_nthreads(1)


class WorkerNode(object):

    def __init__(self, data_dir=bqueryd.DEFAULT_DATA_DIR, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.DEBUG):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception("Datadir %s is not a valid difrectory" % data_dir)
        self.worker_id = binascii.hexlify(os.urandom(8))
        self.node_name = socket.gethostname()
        self.data_dir = data_dir
        self.data_files = set()
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.identity = self.worker_id
        self.redis_server = redis.from_url(redis_url)
        self.controllers = {} # Keep a dict of timestamps when you last spoke to controllers
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.check_controllers()
        self.last_wrm = 0
        self.start_time = time.time()
        self.logger = bqueryd.logger.getChild('worker '+self.worker_id)
        self.logger.setLevel(loglevel)

    def check_controllers(self):
        # Check the Redis set of controllers to see if any new ones have appeared,
        # Also register with them if so.
        listed_controllers = list(self.redis_server.smembers(bqueryd.REDIS_SET_KEY))
        current_controllers = []
        new_controllers = []
        for k in self.controllers.keys()[:]:
            if k not in listed_controllers:
                del self.controllers[k]
                self.socket.disconnect(k)
            else:
                current_controllers.append(k)

        new_controllers = [c for c in listed_controllers if c not in current_controllers]
        for controller_address in new_controllers:
            self.socket.connect(controller_address)
            self.controllers[controller_address] = {'last_seen': 0, 'last_sent': 0, 'address': controller_address}

    def check_datafiles(self):
        has_new_files = False
        for data_file in [filename for filename in os.listdir(self.data_dir) if
                          filename.endswith(DATA_FILE_EXTENSION) or filename.endswith(DATA_SHARD_FILE_EXTENSION)]:
            if data_file not in self.data_files:
                self.data_files.add(data_file)
                has_new_files = True
        if len(self.data_files) < 1:
            self.logger.debug('Data directory %s has no files like %s or %s' % (
                self.data_dir, DATA_FILE_EXTENSION, DATA_SHARD_FILE_EXTENSION))
        return has_new_files

    def prepare_wrm(self):
        wrm = WorkerRegisterMessage()
        wrm['worker_id'] = self.worker_id
        wrm['node'] = self.node_name
        wrm['data_files'] = list(self.data_files)
        wrm['data_dir'] = self.data_dir
        wrm['controllers'] = self.controllers.values()
        wrm['uptime'] = int(time.time() - self.start_time)
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
                    self.socket.send_multipart([controller, wrm.to_json()])
                    data['last_sent'] = time.time()
                    # self.logger.debug("heartbeat to %s" % data['address'])

    def go(self):
        self.logger.debug('Starting')
        self.running = True
        while self.running:
            self.heartbeat()
            for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                if event & zmq.POLLIN:
                    tmp = self.socket.recv_multipart()
                    if len(tmp) != 2:
                        self.logger.critical('Received a msg with len != 2, something seriously wrong. ')
                        continue
                    sender, msg_buf = tmp
                    msg = msg_factory(msg_buf)

                    data = self.controllers.get(sender)
                    if not data:
                        self.logger.critical('Received a msg from %s - this is an unknown sender' % sender)
                        continue
                    data['last_seen'] = time.time()

                    # self.logger.debug('Received from %s' % sender)
                    # TODO Notify Controllers that we are busy, no more messages to be sent
                    self.send_to_all(BusyMessage())
                    # The above busy notification is not perfect as other messages might be on their way already
                    # but for long-running queries it will at least ensure other controllers
                    # don't try and overuse this node by filling up a queue
                    try:
                        tmp = self.handle(msg)
                    except Exception, e:
                        tmp = ErrorMessage(msg)
                        tmp['payload'] = traceback.format_exc()
                        self.logger.debug(tmp['payload'])
                    self.send_to_all(DoneMessage()) # Send an empty mesage to all controllers, this flags you as 'Done'
                    if tmp:
                        self.socket.send_multipart([sender, tmp.to_json()])
        self.logger.debug('Stopping')

    def send_to_all(self, msg):
        for controller in self.controllers:
            self.socket.send_multipart([controller, msg.to_json()])

    def handle_calc(self, msg):
        args, kwargs = msg.get_args_kwargs()
        self.logger.debug('doing calc %s' % args)
        filename = args[0]
        groupby_col_list = args[1]
        aggregation_list = args[2]
        where_terms_list = args[3]
        expand_filter_column = kwargs.get('expand_filter_column')
        aggregate = kwargs.get('aggregate', True)

        # create rootdir
        rootdir = os.path.join(self.data_dir, filename)
        if not os.path.exists(rootdir):
            raise Exception('Path %s does not exist' % rootdir)

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

    def file_downloader_callback(self, msg):
        def _fn(progress, size):
            args, kwargs = msg.get_args_kwargs()
            filename = kwargs.get('filename')
            ticket = msg.get('ticket')
            addr = str(msg.get('source'))
            if progress == 0:
                self.logger.debug('At %s of %s for %s %s :: %s' % (progress, size, ticket, addr, filename))
            tmp = FileDownloadProgress(msg)
            tmp['filename'] = filename
            tmp['progress'] = progress
            tmp['size'] = size
            self.socket.send_multipart([addr, tmp.to_json()])
        return _fn

    def handle_download(self, msg):
        args, kwargs = msg.get_args_kwargs()
        filename = kwargs.get('filename')
        bucket = kwargs.get('bucket')

        if not (filename and bucket):
            raise Exception('[filename, bucket] args are all required')

        # get file from S3
        s3_conn = boto.connect_s3()
        s3_bucket = s3_conn.get_bucket(bucket, validate=False)
        k = s3_bucket.get_key(filename, validate=False)
        fd, tmp_filename = tempfile.mkstemp(dir=bqueryd.INCOMING)

        the_callback = self.file_downloader_callback(msg)
        k.get_contents_to_filename(tmp_filename, cb=the_callback)

        # unzip the tmp file to the filename
        ticket = msg['ticket']
        #ticket_path = os.path.join(bqueryd.INCOMING, ticket)
        #if not os.path.exists(ticket_path):
        #    os.mkdir(ticket_path)

        # temp_path = os.path.join(bqueryd.INCOMING, ticket, filename)
        temp_path = os.path.join(bqueryd.DEFAULT_DATA_DIR, filename)
        # if the signature already exists, first remove it.
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path, ignore_errors=True)
        with zipfile.ZipFile(tmp_filename, 'r', allowZip64=True) as myzip:
            myzip.extractall(temp_path)
        self.logger.debug("Downloaded %s" % temp_path)
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)
        msg.add_as_binary('result', temp_path)
        the_callback(-1, -1)

        return msg

    def handle_movebcolz(self, msg):
        # A notification from the controller that all files are downloaded on all nodes, the files in this ticket can be moved into place
        self.logger.debug('movebcolz %s' % msg['ticket'])
        ticket = msg['ticket']
        ticket_path = os.path.join(bqueryd.INCOMING, ticket)
        if not os.path.exists(ticket_path):
            return

        for filename in os.listdir(ticket_path):
            prod_path = os.path.join(bqueryd.DEFAULT_DATA_DIR, filename)
            if os.path.exists(prod_path):
                shutil.rmtree(prod_path, ignore_errors=True)
            ready_path = os.path.join(ticket_path, filename)
            self.logger.debug("moving %s %s" % (ready_path, prod_path))
            os.rename(ready_path, prod_path)

        shutil.rmtree(ticket_path, ignore_errors=True)
        # TODO add some error handling when something goes wrong in this movemessage, send the exception to the
        # calling controller and hang it on the download ticket


    def handle(self, msg):
        if msg.isa('kill'):
            self.running = False
            # Also send a message to all your controllers, that you are stopping
            self.send_to_all(StopMessage())
            for k in self.controllers:
                self.socket.disconnect(k)
            return
        elif msg.isa('info'):
            msg = self.prepare_wrm()
        elif msg.isa('sleep'):
            args, kwargs = msg.get_args_kwargs()
            time.sleep(float(args[0]))
            snore = 'z'*random.randint(1,20)
            self.logger.debug(snore)
            msg.add_as_binary('result', snore)
        elif msg.isa('download'):
            msg = self.handle_download(msg)
        elif msg.isa('movebcolz'):
            msg = self.handle_movebcolz(msg)
        else:
            msg = self.handle_calc(msg)
        return msg
