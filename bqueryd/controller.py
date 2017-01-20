import time
import zmq
import logging
import binascii
import traceback
import json
import random
import os
import socket
import pandas as pd
import redis
import bqueryd
from bqueryd.messages import msg_factory, Message, WorkerRegisterMessage, ErrorMessage, BusyMessage, DoneMessage, StopMessage
from bqueryd.util import get_my_ip, bind_to_random_port

POLLING_TIMEOUT = 5000  # timeout in ms : how long to wait for network poll, this also affects frequency of seeing new nodes
DEAD_WORKER_TIMEOUT = 1 * 60 # time in seconds that we wait for a worker to respond before being removed
HEARTBEAT_INTERVAL = 15 # time in seconds between doing heartbeats


class ControllerNode(object):

    def __init__(self, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.DEBUG):

        self.redis_url = redis_url
        self.redis_server = redis.from_url(redis_url)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.ROUTER_MANDATORY, 1) # Paranoid for debugging purposes
        self.socket.setsockopt(zmq.SNDTIMEO, 1000) # Short timeout
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN | zmq.POLLOUT)

        self.node_name = socket.gethostname()
        self.address = bind_to_random_port(self.socket, 'tcp://'+get_my_ip(), min_port=14300, max_port=14399, max_tries=100)
        self.logger = bqueryd.logger.getChild('controller').getChild(self.address)
        self.logger.setLevel(loglevel)

        self.msg_count_in = 0
        self.rpc_results = []  # buffer of results that are ready to be returned to callers
        self.rpc_segments = {}  # Certain RPC calls get split and divided over workers, this dict tracks the original RPCs
        self.worker_map = {}  # maintain a list of connected workers TODO get rid of unresponsive ones...
        self.files_map = {}  # shows on which workers a file is available on
        self.worker_out_messages = []
        self.is_running = True
        self.last_heartbeat = 0
        # Keep a list of files presently being downloaded
        self.downloads = {}
        self.others = {} # A dict of other Controllers running on other DQE nodes

    def send(self, addr, msg_buf, is_rpc=False):
        try:
            if is_rpc:
                tmp = [addr, '', msg_buf]
            else:
                tmp = [addr, msg_buf]
            self.socket.send_multipart(tmp)
        except zmq.ZMQError, ze:
            self.logger.debug("Problem with %s: %s" % (addr, ze))

    def connect_to_others(self):
        # Make sure own address is still registered
        self.redis_server.sadd(bqueryd.REDIS_SET_KEY, self.address)
        # get list of other running controllers and connect to them
        all_servers = self.redis_server.smembers(bqueryd.REDIS_SET_KEY)
        all_servers.remove(self.address)
        # Connect to new other controllers we don't know about yet
        for x in all_servers:
            if x not in self.others:
                self.logger.debug('Connecting to %s' % x)
                self.socket.connect(x)
                self.others[x] = {'connect_time': time.time()}
        # Disconnect from controllers not in current set
        for x in self.others.keys()[:]: # iterate over a copy of keys so we can remove entries
            if x not in all_servers:
                self.logger.debug('Disconnecting from %s' % x)
                try:
                    del self.others[x]
                    self.socket.disconnect(x)
                except zmq.error.ZMQError, e:
                    self.logger.exception(e)

    def heartbeat(self):
        if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
            self.connect_to_others()
            self.last_heartbeat = time.time()

    def find_local_worker(self):
        tmp = [worker_id for worker_id, worker in self.worker_map.items() if worker.get('node') == self.node_name]
        if tmp:
            return tmp[0]

    def find_free_worker(self):
        # Pick a random worker_id to send a message to for now, TODO add some kind of load-balancing & affinity
        free_workers = [worker_id for worker_id, worker in self.worker_map.items() if not worker.get('busy', False)]
        if not free_workers:
            return None
        return random.choice(free_workers)

    def process_sink_results(self):
        while self.rpc_results:
            msg = self.rpc_results.pop()
            # If this was a message to be combined, there should be a parent_token
            if 'parent_token' in msg:
                parent_token = msg['parent_token']
                if parent_token not in self.rpc_segments:
                    logging.debug('Orphaned msg segment %s probabably an error was raised elsewhere' % parent_token)
                    continue
                original_rpc = self.rpc_segments.get(parent_token)

                if isinstance(msg, ErrorMessage):
                    self.logger.debug('Errormesssage %s' % msg.get('payload'))
                    # Delete this entire message segments, if it still exists
                    if parent_token in self.rpc_segments:
                        del self.rpc_segments[parent_token]
                        # If any of the segment workers return an error,
                        # Send the exception on to the calling RPC
                        msg['token'] = parent_token
                        msg_id = binascii.unhexlify(parent_token)
                        self.send(msg_id, msg.to_json(), is_rpc=True)
                    # and continue the processing
                    continue

                args, kwargs = msg.get_args_kwargs()
                filename = args[0]
                original_rpc['results'][filename] = msg.get_from_binary('result')

                if len(original_rpc['results']) == len(original_rpc['filenames']):
                    # Check to see that there are no filenames with no Result yet
                    # TODO as soon as any workers gives an error abort the whole enchilada

                    # if finished, aggregate the result
                    result_list = original_rpc['results'].values()
                    new_result = create_result_from_response({'args':args, 'kwargs':kwargs}, result_list)
                    # We have received all the segment, send a reply to RPC caller
                    msg = original_rpc['msg']
                    msg.add_as_binary('result', new_result)
                    del self.rpc_segments[parent_token]
                else:
                    # This was a segment result move on
                    continue

            msg_id = binascii.unhexlify(msg.get('token'))
            self.send(msg_id, msg.to_json(), is_rpc=True)
            self.logger.debug('Msg handled %s' % msg.get('token', '?'))

    def handle_out(self):
        self.process_sink_results()

        while self.worker_out_messages:

            msg = self.worker_out_messages.pop()
            if 'worker_id' in msg:
                worker_id = msg['worker_id']
            else:
                # Assumption now is that all workers can serve requests to all files,
                # TODO We need to change the find_free_worker to take the requested filename into account
                # find a worker that is free
                worker_id = self.find_free_worker()
                if not worker_id:
                    self.logger.debug('No free workers at this time')
                    self.worker_out_messages.append(msg)
                    break

            # TODO Add a tracking of which requests have been sent out to the worker, and do retries with timeouts
            self.worker_map[worker_id]['last_sent'] = time.time()
            self.worker_map[worker_id]['busy'] = True
            self.send(worker_id, msg.to_json())

    def handle_in(self):
        self.msg_count_in += 1
        data = self.socket.recv_multipart()
        if len(data) == 3: # This is a RPC call from a zmq.REQ socket
            sender, _blank, msg_buf = data
            self.handle_rpc(sender, msg_factory(msg_buf))
        elif len(data) == 2: # This is an internode call from another zmq.ROUTER, a Controller or Worker
            sender, msg_buf = data
            msg = msg_factory(msg_buf)
            if sender in self.others:
                self.handle_peer(sender, msg)
            else:
                self.handle_worker(sender, msg)

    def handle_peer(self, sender, msg):
        self.logger.debug('****** %s' % msg)
        if msg.isa('download'):
            self.handle_download(msg)
        else:
            self.logger.debug("Got a msg but don't know what to do with it %s" % msg)

    def handle_worker(self, worker_id, msg):

        # TODO Make a distinction on the kind of message received and act accordingly
        msg['worker_id'] = worker_id

        # TODO If worker not in worker_map due to a dead_worker cull (calculation too a long time...)
        # request a new WorkerRegisterMessage from that worker...
        if not msg.isa(WorkerRegisterMessage) and worker_id not in self.worker_map:
            self.send(worker_id, Message({'payload': 'info'}).to_json())

        self.worker_map.setdefault(worker_id, {})['last_seen'] = time.time()

        if msg.isa(WorkerRegisterMessage):
            self.logger.debug('Worker registered %s' % worker_id)
            for filename in msg.get('data_files', []):
                self.files_map.setdefault(filename, set()).add(worker_id)
            self.worker_map[worker_id]['node'] = msg['node']
            return

        if msg.isa(BusyMessage):
            self.logger.debug('Worker %s sent BusyMessage' % worker_id)
            self.worker_map[worker_id]['busy'] = True
            return

        if msg.isa(DoneMessage):
            self.logger.debug('Worker %s sent DoneMessage' % worker_id)
            self.worker_map[worker_id]['busy'] = False
            return

        if msg.isa(StopMessage):
            self.remove_worker(worker_id)
            return

        # Every msg needs a token, otherwise we don't know who the reply goes to
        if 'token' not in msg:
            self.logger.debug('Message received without a token in it?')
            return

        # A message might have been passed on to a worker for processing and needs to be returned to the relevant caller
        # so it goes in the rpc_results list
        self.rpc_results.append(msg)

    def handle_rpc(self, sender, msg):
        # RPC calls have a binary identiy set, hexlify it to make it readable and serializable
        msg_id = binascii.hexlify(sender)
        msg['token'] = msg_id
        self.logger.debug('RPC received %s' % msg_id)

        result = "Sorry, I don't understand you"
        if msg.isa('ping'):
            result = 'pong'
        elif msg.isa('info'):
            result = self.get_info()
        elif msg.isa('kill'):
            result = self.kill()
        elif msg.isa('killworkers'):
            result = self.killworkers()
        elif msg.isa('download'):
            for o in self.others:
                mm = msg.copy()
                del mm['token']
                self.send(o, mm.to_json())
            result = self.handle_download(msg)
        elif msg['payload'] in ('sleep',):
            self.worker_out_messages.append(msg.copy())
            result = None
        elif msg['payload'] in ('groupby',):
            result = self.handle_calc_message(msg)  # if result is not None something happened, return to caller immediately

        if result:
            msg.add_as_binary('result', result)
            self.rpc_results.append(msg)

    def handle_download(self, msg):
        # the caller either wants to wait for the entire download to be done,
        # or get a ticket and get to query with status updates
        a_local = self.find_local_worker()
        if not a_local:
            raise Exception('No local worker found for download message!')
        msg['worker_id'] = a_local
        self.worker_out_messages.append(msg)

    def handle_calc_message(self, msg):
        args, kwargs = msg.get_args_kwargs()

        if len(args) != 4:
            return 'Error, No correct args given, expecting: ' + \
                   'path_list, groupby_col_list, measure_col_list, where_terms_list'

        filenames = args[0]
        if not filenames:
            return 'Error, no filenames given'

        # Make sure that all filenames are available before any messages are sent
        for filename in filenames:
            if filename and filename not in self.files_map:
                return 'Sorry, filename %s was not found' % filename

        parent_token = msg['token']
        rpc_segment = {'msg': msg_factory(msg.copy()),
                       'results': {},
                       'filenames': dict([(x, None) for x in filenames])}

        params = {}
        for filename in filenames:
            params['args'] = list(args)
            params['args'][0] = filename
            msg.add_as_binary('params', params)

            # Make up a new token for the message sent to the workers, and collect the responses using that id
            msg['parent_token'] = parent_token
            new_token = binascii.hexlify(os.urandom(8))
            msg['token'] = new_token
            rpc_segment['filenames'][filename] = new_token
            self.worker_out_messages.append(msg.copy())

        self.rpc_segments[parent_token] = rpc_segment

    def kill(self):
        #unregister with the Redis set
        self.redis_server.srem(bqueryd.REDIS_SET_KEY, self.address)
        self.is_running = False
        return 'harakiri...'

    def killworkers(self):
        # Send a kill message to each of our workers
        for x in self.worker_map:
            self.send(x, Message({'payload': 'kill', 'token': 'kill'}).to_json())
        return 'hai!'

    def get_info(self):
        data = {'msg_count_in': self.msg_count_in,
                'workers': self.worker_map,
                'last_heartbeat': self.last_heartbeat, 'address': self.address,
                'others': self.others
                }
        return data

    def remove_worker(self, worker_id):
        self.logger.debug("Removing worker %s" % worker_id)
        if worker_id in self.worker_map:
            del self.worker_map[worker_id]
        for worker_set in self.files_map.values():
            if worker_id in worker_set:
                worker_set.remove(worker_id)

    def free_dead_workers(self):
        now = time.time()
        for worker_id, worker in self.worker_map.items():
            if (now - worker.get('last_seen', now)) > DEAD_WORKER_TIMEOUT:
                self.remove_worker(worker_id)

    def go(self):
        self.logger.debug('Started')

        while self.is_running:
            try:
                time.sleep(0.0001)
                self.heartbeat()
                self.free_dead_workers()
                for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                    if event & zmq.POLLIN:
                        self.handle_in()
                    if event & zmq.POLLOUT:
                        self.handle_out()

            except KeyboardInterrupt:
                self.logger.debug('Keyboard Interrupt')
                self.kill()
            except:
                self.logger.error("Exception %s" % traceback.format_exc())

        self.logger.debug('Stopping')

def create_result_from_response(params, result_list):
    if not result_list:
        new_result = pd.DataFrame()
    elif len(result_list) == 1:
        new_result = result_list[0]
    else:
        new_result = pd.concat(result_list, ignore_index=True)

        if params.get('kwargs', {}).get('aggregate', True) and not new_result.empty:
            groupby_cols = params['args'][1]
            aggregation_list = params['args'][2]
            if not groupby_cols:
                new_result = pd.DataFrame(new_result.sum()).transpose()
            elif aggregation_list:
                # aggregate over totals if needed
                measure_cols = [x[2] for x in aggregation_list]
                new_result = new_result.groupby(groupby_cols, as_index=False)[measure_cols].sum()

    return new_result
