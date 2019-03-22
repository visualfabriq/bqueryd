import binascii
import errno
import gc
import importlib
import json
import logging
import os
import random
import tarfile
import tempfile
import traceback
import zipfile

import bcolz
import boto3
import bquery
import psutil
import redis
import shutil
import signal
import smart_open
import socket
import time
import zmq
from ssl import SSLError

import bqueryd
from bqueryd.messages import msg_factory, WorkerRegisterMessage, ErrorMessage, BusyMessage, StopMessage, \
    DoneMessage, TicketDoneMessage
from bqueryd.tool import rm_file_or_dir

DATA_FILE_EXTENSION = '.bcolz'
DATA_SHARD_FILE_EXTENSION = '.bcolzs'
# timeout in ms : how long to wait for network poll, this also affects frequency of seeing new controllers and datafiles
POLLING_TIMEOUT = 5000
# how often in seconds to send a WorkerRegisterMessage
WRM_DELAY = 20
MAX_MEMORY_KB = 2 * (2 ** 20)     # Max memory of 2GB, in Kilobytes
DOWNLOAD_DELAY = 5  # how often in seconds to check for downloads
bcolz.set_nthreads(1)


class WorkerBase(object):
    def __init__(self, data_dir=bqueryd.DEFAULT_DATA_DIR, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.DEBUG,
                 restart_check=True):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception("Datadir %s is not a valid directory" % data_dir)
        self.worker_id = binascii.hexlify(os.urandom(8))
        self.node_name = socket.gethostname()
        self.data_dir = data_dir
        self.data_files = set()
        self.restart_check = restart_check
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 500)
        self.socket.identity = self.worker_id
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN | zmq.POLLOUT)
        self.redis_server = redis.from_url(redis_url)
        self.controllers = {}  # Keep a dict of timestamps when you last spoke to controllers
        self.check_controllers()
        self.last_wrm = 0
        self.start_time = time.time()
        self.logger = bqueryd.logger.getChild('worker ' + self.worker_id)
        self.logger.setLevel(loglevel)
        self.msg_count = 0
        signal.signal(signal.SIGTERM, self.term_signal())
        self.running = False

    def term_signal(self):
        def _signal_handler(signum, frame):
            self.logger.info("Received TERM signal, stopping.")
            self.running = False

        return _signal_handler

    def send(self, addr, msg):
        try:
            if 'data' in msg:
                data = msg['data']
                del msg['data']
                self.socket.send_multipart([addr, msg.to_json(), data])
            else:
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
        replacement_data_files = set()
        for data_file in [filename for filename in os.listdir(self.data_dir) if
                          filename.endswith(DATA_FILE_EXTENSION) or filename.endswith(DATA_SHARD_FILE_EXTENSION)]:
            if data_file not in self.data_files:
                has_new_files = True
            replacement_data_files.add(data_file)
        self.data_files = replacement_data_files
        return has_new_files

    def prepare_wrm(self):
        wrm = WorkerRegisterMessage()
        wrm['worker_id'] = self.worker_id
        wrm['node'] = self.node_name
        wrm['data_files'] = list(self.data_files)
        wrm['data_dir'] = self.data_dir
        wrm['controllers'] = self.controllers.values()
        wrm['uptime'] = int(time.time() - self.start_time)
        wrm['msg_count'] = self.msg_count
        wrm['pid'] = os.getpid()
        wrm['workertype'] = self.workertype
        return wrm

    def heartbeat(self):
        time.sleep(0.001)  # to prevent too tight loop
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
                    self.logger.debug("heartbeat to %s", data['address'])

    def handle_in(self):
        try:
            tmp = self.socket.recv_multipart()
        except zmq.Again:
            return
        if len(tmp) != 2:
            self.logger.critical('Received a msg with len != 2, something seriously wrong. ')
            return
        sender, msg_buf = tmp
        self.logger.info("Received message from sender %s", sender)
        msg = msg_factory(msg_buf)

        data = self.controllers.get(sender)
        if not data:
            self.logger.critical('Received a msg from %s - this is an unknown sender' % sender)
            return
        data['last_seen'] = time.time()
        self.logger.debug('Received from %s', sender)

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
            self.logger.exception("Unable to handle message [%s]", msg)
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

        # Also send a message to all your controllers, that you are stopping
        self.send_to_all(StopMessage())
        for k in self.controllers:
            self.socket.disconnect(k)

        self.logger.info('Stopping')

    def send_to_all(self, msg):
        for controller in self.controllers:
            self.send(controller, msg)

    def handle(self, msg):
        if msg.isa('kill'):
            self.running = False
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
        elif msg.isa('readfile'):
            args, kwargs = msg.get_args_kwargs()
            msg['data'] = open(args[0]).read()
        elif msg.isa('sleep'):
            args, kwargs = msg.get_args_kwargs()
            time.sleep(float(args[0]))
            snore = 'z' * random.randint(1, 20)
            self.logger.debug(snore)
            msg.add_as_binary('result', snore)
        else:
            msg = self.handle_work(msg)
            self.msg_count += 1
            gc.collect()
            self._check_mem(msg)

        return msg

    def _check_mem(self, msg):
        # RSS is in bytes, convert to Kilobytes
        rss_kb = psutil.Process().memory_full_info().rss / (2 ** 10)
        self.logger.debug("RSS is: %s KB", rss_kb)
        if self.restart_check and rss_kb > MAX_MEMORY_KB:
            args = msg.get_args_kwargs()[0]
            self.logger.critical('args are: %s', args)
            self.logger.critical(
                'Memory usage (KB) %s > %s, restarting', rss_kb, MAX_MEMORY_KB)
            self.running = False

    def handle_work(self, msg):
        raise NotImplementedError


class WorkerNode(WorkerBase):
    workertype = 'calc'

    def execute_code(self, msg):
        args, kwargs = msg.get_args_kwargs()

        func_fully_qualified_name = kwargs['function'].split('.')
        module_name = '.'.join(func_fully_qualified_name[:-1])
        func_name = func_fully_qualified_name[-1]
        func_args = kwargs.get("args", [])
        func_kwargs = kwargs.get("kwargs", {})

        self.logger.debug('Importing module: %s' % module_name)
        mod = importlib.import_module(module_name)
        function = getattr(mod, func_name)
        self.logger.debug('Executing function: %s' % func_name)
        self.logger.debug('args: %s kwargs: %s' % (func_args, func_kwargs))
        result = function(*func_args, **func_kwargs)

        msg.add_as_binary('result', result)
        return msg

    def handle_work(self, msg):
        if msg.isa('execute_code'):
            return self.execute_code(msg)

        tmp_dir = tempfile.mkdtemp(prefix='result_')
        buf_file_fd, buf_file = tempfile.mkstemp(prefix='tar_')
        os.close(buf_file_fd)

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
                msg['data'] = ''
                return msg
            # else create the boolean array
            bool_arr = ct.where_terms(where_terms_list, cache=True)

        # expand filter column check
        if expand_filter_column:
            bool_arr = ct.is_in_ordered_subgroups(basket_col=expand_filter_column, bool_arr=bool_arr)

        # retrieve & aggregate if needed
        rm_file_or_dir(tmp_dir)
        if aggregate:
            # aggregate by groupby parameters
            result_ctable = ct.groupby(groupby_col_list, aggregation_list, bool_arr=bool_arr,
                                       rootdir=tmp_dir)
        else:
            # direct result from the ctable
            column_list = groupby_col_list + [x[0] for x in aggregation_list]
            if bool_arr is not None:
                result_ctable = bcolz.fromiter(ct[column_list].where(bool_arr), ct[column_list].dtype, sum(bool_arr),
                                               rootdir=tmp_dir, mode='w')
            else:
                result_ctable = bcolz.fromiter(ct[column_list], ct[column_list].dtype, ct.len,
                                               rootdir=tmp_dir, mode='w')

        # *** clean up temporary files and memory objects
        # filter
        del bool_arr

        # input
        ct.free_cachemem()
        ct.clean_tmp_rootdir()
        del ct

        # save result to archive
        result_ctable.flush()
        result_ctable.free_cachemem()
        with tarfile.open(buf_file, mode='w') as archive:
            archive.add(tmp_dir, arcname=os.path.basename(tmp_dir))
        del result_ctable
        rm_file_or_dir(tmp_dir)

        # create message
        with open(buf_file, 'r') as file:
            # add result to message
            msg['data'] = file.read()
        rm_file_or_dir(buf_file)

        return msg


class DownloaderNode(WorkerBase):
    workertype = 'download'

    def __init__(self, *args, **kwargs):
        super(DownloaderNode, self).__init__(*args, **kwargs)
        self.last_download_check = 0

    def heartbeat(self):
        super(DownloaderNode, self).heartbeat()
        if time.time() - self.last_download_check > DOWNLOAD_DELAY:
            self.check_downloads()

    def check_downloads(self):
        # Note, the files being downloaded are stored per key on filename,
        # Yet files are grouped as being inside a ticket for downloading at the same time
        # this done so that a group of files can be synchronized in downloading
        # when called from rpc.download(filenames=[...]

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(bqueryd.REDIS_TICKET_KEY_PREFIX + '*')

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(bqueryd.REDIS_TICKET_KEY_PREFIX):]

            ticket_details_items = [(k, v) for k, v in ticket_details.items()]
            random.shuffle(ticket_details_items)

            for node_filename_slot, progress_slot in ticket_details_items:
                tmp = node_filename_slot.split('_')
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                filename = '_'.join(tmp[1:])

                tmp = progress_slot.split('_')
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                timestamp, progress = float(tmp[0]), tmp[1]

                if node != self.node_name:
                    continue

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress == 'DONE':
                    continue

                try:
                    # acquire a lock for this node_filename
                    lock_key = bqueryd.REDIS_DOWNLOAD_LOCK_PREFIX + self.node_name + ticket + filename
                    lock = self.redis_server.lock(lock_key, timeout=bqueryd.REDIS_DOWNLOAD_LOCK_DURATION)
                    if lock.acquire(False):
                        self.download_file(ticket, filename)
                        break  # break out of the loop so we don't end up staying in a large loop for giant tickets
                except:
                    self.logger.exception('Problem downloading %s %s', ticket, filename)
                    # Clean up the whole ticket if an error occured
                    self.remove_ticket(ticket)
                    break
                finally:
                    try:
                        lock.release()
                    except redis.lock.LockError:
                        pass

    def file_downloader_progress(self, ticket, filename, progress):
        node_filename_slot = '%s_%s' % (self.node_name, filename)
        # Check to see if the progress slot exists at all, if it does not exist this ticket has been cancelled
        # by some kind of intervention, stop the download and clean up.
        tmp = self.redis_server.hget(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        if not tmp:
            # Clean up the whole ticket contents from disk
            ticket_path = os.path.join(bqueryd.INCOMING, ticket)
            self.logger.debug('Now removing entire ticket %s', ticket_path)
            shutil.rmtree(ticket_path, ignore_errors=True)
            raise Exception("Ticket %s progress slot %s not found, aborting download" % (ticket, node_filename_slot))
        # A progress slot contains a timestamp_filesize
        progress_slot = '%s_%s' % (time.time(), progress)
        self.redis_server.hset(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

    def _get_transport_params(self):
        return {}

    def download_file(self, ticket, fileurl):
        tmp = fileurl[5:].split('/')
        bucket = tmp[0]
        filename = '/'.join(tmp[1:])
        ticket_path = os.path.join(bqueryd.INCOMING, ticket)
        if not os.path.exists(ticket_path):
            try:
                os.makedirs(ticket_path)
            except OSError, ose:
                if ose == errno.EEXIST:
                    pass  # different processes might try to create the same directory at _just_ the same time causing the previous check to fail
        temp_path = os.path.join(bqueryd.INCOMING, ticket, filename)

        if os.path.exists(temp_path):
            self.logger.info("%s exists, skipping download" % temp_path)
            self.file_downloader_progress(ticket, fileurl, 'DONE')
        else:
            self.logger.info("Downloading ticket [%s], bucket [%s], filename [%s]" % (ticket, bucket, filename))

            access_key, secret_key, s3_conn = self._get_s3_conn()
            object_summary = s3_conn.Object(bucket, filename)
            size = object_summary.content_length

            key = 's3://{}:{}@{}/{}'.format(access_key, secret_key, bucket, filename)

            try:
                fd, tmp_filename = tempfile.mkstemp(dir=bqueryd.INCOMING)

                # See: https://github.com/RaRe-Technologies/smart_open/commit/a751b7575bfc5cc277ae176cecc46dbb109e47a4
                # Sometime we get timeout errors on the SSL connections
                for x in range(3):
                    try:
                        transport_params = self._get_transport_params()
                        with smart_open.smart_open(key, 'rb', **transport_params) as fin:
                            buf = True
                            progress = 0
                            while buf:
                                buf = fin.read(pow(2, 20) * 16)  # Use a bigger buffer
                                os.write(fd, buf)
                                progress += len(buf)
                                self.file_downloader_progress(ticket, fileurl, size)
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
                self.logger.debug("Downloaded %s" % tmp_filename)
            finally:
                if os.path.exists(tmp_filename):
                    os.remove(tmp_filename)
                os.close(fd)
        self.logger.debug('Download done %s s3://%s/%s', ticket, bucket, filename)
        self.file_downloader_progress(ticket, fileurl, 'DONE')

    def _get_s3_conn(self):
        """Create a boto3 """
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            raise ValueError('Missing S3 credentials')
        credentials = credentials.get_frozen_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        s3_conn = boto3.resource('s3')
        return access_key, secret_key, s3_conn

    def remove_ticket(self, ticket):
        # Remove all Redis entries for this node and ticket
        # it can't be done per file as we don't have the bucket name from which a file was downloaded
        self.logger.debug('Removing ticket %s from redis', ticket)
        for node_filename_slot in self.redis_server.hgetall(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket):
            if node_filename_slot.startswith(self.node_name):
                self.logger.debug('Removing ticket_%s %s', ticket, node_filename_slot)
                self.redis_server.hdel(bqueryd.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        tdm = TicketDoneMessage({'ticket': ticket})
        self.send_to_all(tdm)


class MoveBcolzNode(DownloaderNode):
    workertype = 'movebcolz'

    def movebcolz(self, ticket):
        # A notification from the controller that all files are downloaded on all nodes,
        # the files in this ticket can be moved into place
        ticket_path = os.path.join(bqueryd.INCOMING, ticket)
        if os.path.exists(ticket_path):
            for filename in os.listdir(ticket_path):
                prod_path = os.path.join(bqueryd.DEFAULT_DATA_DIR, filename)
                if os.path.exists(prod_path):
                    shutil.rmtree(prod_path, ignore_errors=True)
                ready_path = os.path.join(ticket_path, filename)
                # Add a metadata file to the downloaded item
                metadata = {'ticket': ticket, 'timestamp': time.time(), 'localtime': time.ctime()}
                metadata_filepath = os.path.join(ready_path, 'bqueryd.metadata')
                open(metadata_filepath, 'w').write(json.dumps(metadata, indent=2))
                self.logger.debug("Moving %s %s" % (ready_path, prod_path))
                shutil.move(ready_path, prod_path)
            self.logger.debug('Now removing entire ticket %s', ticket_path)
            shutil.rmtree(ticket_path, ignore_errors=True)
        else:
            self.logger.debug('Doing a movebcolz for path %s which does not exist', ticket_path)

    def check_downloads(self):
        # Check all the entries for a specific ticket over all the nodes
        # only if ALL the nodes are _DONE downloading, move the bcolz files in this ticket into place.

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(bqueryd.REDIS_TICKET_KEY_PREFIX + '*')

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(bqueryd.REDIS_TICKET_KEY_PREFIX):]

            in_progress_count = 0
            ticket_details_items = ticket_details.items()

            ticket_on_this_node = False

            for node_filename_slot, progress_slot in ticket_details_items:
                tmp = node_filename_slot.split('_')
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                filename = '_'.join(tmp[1:])

                tmp = progress_slot.split('_')
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                timestamp, progress = float(tmp[0]), tmp[1]

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress != 'DONE':
                    in_progress_count += 1

                if node == self.node_name:
                    ticket_on_this_node = True

            if in_progress_count == 0 and ticket_on_this_node:
                try:
                    self.movebcolz(ticket)
                except:
                    self.logger.exception('Error occurred in movebcolz %s', ticket)
                finally:
                    self.remove_ticket(ticket)
