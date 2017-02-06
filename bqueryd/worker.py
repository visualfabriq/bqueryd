import os
import errno
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
import smart_open
from ssl import SSLError
import pandas as pd
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
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN|zmq.POLLOUT)
        self.redis_server = redis.from_url(redis_url)
        self.controllers = {} # Keep a dict of timestamps when you last spoke to controllers
        self.check_controllers()
        self.last_wrm = 0
        self.start_time = time.time()
        self.logger = bqueryd.logger.getChild('worker '+self.worker_id)
        self.logger.setLevel(loglevel)

    def send(self, addr, msg):
        try:
            self.socket.send_multipart([addr, msg.to_json()])
        except zmq.ZMQError, ze:
            self.logger.critical("Problem with %s: %s" % (addr, ze))

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
        time.sleep(0.001) # to prevent too tight loop
        since_last_wrm = time.time() - self.last_wrm
        if since_last_wrm > WRM_DELAY:
            self.check_controllers()
            has_new_files = self.check_datafiles()
            self.last_wrm = time.time()
            wrm = self.prepare_wrm()
            for controller, data in self.controllers.items():
                if has_new_files or (time.time() - data['last_seen'] > WRM_DELAY):
                    self.send(controller, wrm)
                    data['last_sent'] = time.time()
                    # self.logger.debug("heartbeat to %s" % data['address'])

    def handle_in(self):
        try:
            tmp = self.socket.recv_multipart()
        except zmq.Again:
            return
        if len(tmp) != 2:
            self.logger.critical('Received a msg with len != 2, something seriously wrong. ')
            return

        sender, msg_buf = tmp
        msg = msg_factory(msg_buf)

        data = self.controllers.get(sender)
        if not data:
            self.logger.critical('Received a msg from %s - this is an unknown sender' % sender)
            return
        data['last_seen'] = time.time()
        # self.logger.debug('Received from %s' % sender)

        # TODO Notify Controllers that we are busy, no more messages to be sent
        # The above busy notification is not perfect as other messages might be on their way already
        # but for long-running queries it will at least ensure other controllers
        # don't try and overuse this node by filling up a queue
        busy_msg = BusyMessage()
        self.send_to_all(busy_msg)

        try:
            tmp = self.handle(msg)
        except Exception, e:
            tmp = ErrorMessage(msg)
            tmp['payload'] = traceback.format_exc()
            self.logger.exception(tmp['payload'])
        if tmp:
            self.send(sender, tmp)

        self.send_to_all(DoneMessage())  # Send a DoneMessage to all controllers, this flags you as 'Done'. Duh

    def go(self):
        self.logger.info('Starting')
        self.running = True
        while self.running:
            self.heartbeat()
            for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                if event & zmq.POLLIN:
                    self.handle_in()
        self.logger.info('Stopping')

    def send_to_all(self, msg):
        for controller in self.controllers:
            self.send(controller, msg)

    def handle_calc(self, msg):
        args, kwargs = msg.get_args_kwargs()
        self.logger.info('doing calc %s' % args)
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

        ct = bquery.ctable(rootdir=rootdir, mode='r', auto_cache=True)

        # prepare filter
        if not where_terms_list:
            bool_arr = None
        else:
            # quickly verify the where_terms_list
            if not ct.where_terms_factorization_check(where_terms_list):
                # return an empty result because the where terms do not give a result for this ctable
                msg.add_as_binary('result', pd.DataFrame())
                return msg
            # else create the boolean array
            bool_arr = ct.where_terms(where_terms_list, cache=True)

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
                result_ctable = bcolz.fromiter(ct[column_list].where(bool_arr), ct[column_list].dtype, sum(bool_arr))
            else:
                result_ctable = bcolz.fromiter(ct[column_list], ct[column_list].dtype, ct.len)
            buf = result_ctable[column_list].todataframe()

        # clean up temporary files and memory objects
        ct.clean_tmp_rootdir()
        del ct
        del result_ctable

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
            self.send(addr, tmp)
        return _fn

    def handle_download(self, msg):
        args, kwargs = msg.get_args_kwargs()
        filename = kwargs.get('filename')
        bucket = kwargs.get('bucket')

        if not (filename and bucket):
            raise Exception('[filename, bucket] args are all required')

        # Used to send progress messages to caller
        the_callback = self.file_downloader_callback(msg)

        ticket = msg['ticket']
        ticket_path = os.path.join(bqueryd.INCOMING, ticket)
        self.logger.info("Downloading %s s3://%s/%s" % (ticket, bucket, filename))
        if not os.path.exists(ticket_path):
            try:
                os.mkdir(ticket_path)
            except OSError, ose:
                if ose == errno.EEXIST:
                    pass # different workers might try to create the same directory at _just_ the same time causing the previous check to fail
        temp_path = os.path.join(bqueryd.INCOMING, ticket, filename)
        if os.path.exists(temp_path):
            self.logger.info("%s exists, skipping download" % temp_path)
        else:
            # Another worker might already be busy downloading the file for this ticket,
            # if that is so just return
            if os.path.exists(temp_path + '.progress'):
                self.logger.debug("%s.progress exists, another download in progress" % temp_path)
                return
            open(temp_path + '.progress', 'w').write(self.worker_id)
            # get file from S3
            s3_conn = boto.connect_s3()
            s3_bucket = s3_conn.get_bucket(bucket, validate=False)
            k = s3_bucket.get_key(filename, validate=False)
            k.open()
            size = k.size
            fd, tmp_filename = tempfile.mkstemp(dir=bqueryd.INCOMING)

            # See: https://github.com/RaRe-Technologies/smart_open/commit/a751b7575bfc5cc277ae176cecc46dbb109e47a4
            # Sometime we get timeout errors on the SSL connections
            for x in range(3):
                try:
                    with smart_open.smart_open(k) as fin, open(tmp_filename, 'w') as fout:
                        buf = True
                        progress = 0
                        while buf:
                            buf = fin.read(pow(2,20) * 16) # Use a bigger buffer
                            fout.write(buf)
                            progress += len(buf)
                            the_callback(progress, size)
                    break
                except SSLError, e:
                    if x == 2:
                        raise e
                    else:
                        pass

            # unzip the tmp file to the filename
            # if temp_path already exists, first remove it.
            if os.path.exists(temp_path):
                shutil.rmtree(temp_path, ignore_errors=True)
            with zipfile.ZipFile(tmp_filename, 'r', allowZip64=True) as myzip:
                myzip.extractall(temp_path)
            self.logger.debug("Downloaded %s" % temp_path)
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
        msg.add_as_binary('result', temp_path)
        self.logger.debug('Download done %s s3://%s/%s', ticket, bucket, filename)
        the_callback(-1, -1)

        return msg

    def handle_movebcolz(self, msg):
        # A notification from the controller that all files are downloaded on all nodes, the files in this ticket can be moved into place
        self.logger.info('movebcolz %s' % msg['ticket'])
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
        elif msg.isa('loglevel'):
            args, kwargs = msg.get_args_kwargs()
            if args:
                loglevel = {'info': logging.INFO, 'debug': logging.DEBUG}.get(args[0], logging.INFO)
                self.logger.setLevel(loglevel)
                self.logger.info("Set loglevel to %s" % loglevel)
            return
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
