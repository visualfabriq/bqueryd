import os
import time
import zmq
import logging
import binascii
import traceback
import json
import random
import pandas as pd
import bqueryd
from bqueryd.messages import msg_factory, Message, WorkerRegisterMessage, ErrorMessage

logger = logging.getLogger('Controller')

class ControllerNode(object):
    def __init__(self):
        self.context = zmq.Context()
        self.rpc = self.context.socket(zmq.ROUTER)
        rpc_address = 'tcp://*:%s' % bqueryd.RPC_PORT
        self.rpc.bind(rpc_address)
        logger.debug('RPC on %s' % rpc_address)

        self.ventilator = self.context.socket(zmq.ROUTER)
        ventilator_address = 'tcp://*:%s' % bqueryd.VENTILATOR_PORT
        self.ventilator.bind(ventilator_address)
        logger.debug('Ventilator on %s' % ventilator_address)

        self.msg_count = 0
        self.rpc_results = []  # buffer of results that are ready to be returned to callers
        self.rpc_segments = {}  # Certain RPC calls get split and divided over workers, this dict tracks the original RPCs
        self.worker_map = {}  # maintain a list of connected workers TODO get rid of unresponsive ones...
        self.files_map = {}  # shows on which workers a file is available on
        self.outgoing_messages = []

    def handle_sink(self, msg):
        if isinstance(msg, WorkerRegisterMessage):
            tmp = {'last_seen': time.time(), 'wrm': msg}
            worker_id = msg.get('worker_id')
            self.worker_map[worker_id] = tmp
            logger.debug('Worker registered %s' % worker_id)
            for filename in msg.get('data_files', []):
                self.files_map.setdefault(filename, set()).add(worker_id)
            return

        # Every msg needs a token, otherwise we don't know who the reply goes to
        if 'token' not in msg:
            logger.error('Every msg needs a token')
            return
        logger.debug('Sink received %s' % msg.get('token', '?'))
        self.rpc_results.append(msg)

    def find_free_worker(self):
        # Pick a random worker_id to send a message to for now, TODO add some kind of load-balancing & affinity
        free_workers = [worker_id for worker_id, worker in self.worker_map.items() if 'last_msg' not in worker]
        if not free_workers:
            return None
        return random.choice(free_workers)

    def go(self):
        self.poller = zmq.Poller()
        self.poller.register(self.ventilator, zmq.POLLIN | zmq.POLLOUT)
        self.poller.register(self.rpc, zmq.POLLIN | zmq.POLLOUT)

        logger.debug('Started')

        socks = {}
        self.running = True
        while self.running:
            try:
                time.sleep(0.0001)  # give the system a breather to stop CPU usage being pegged at 100%
                socks = dict(self.poller.poll())

                if socks.get(self.ventilator) & zmq.POLLIN:
                    buf = self.ventilator.recv_multipart()
                    worker_id, msg = buf[0], buf[1]
                    msg = msg_factory(msg)
                    msg['worker_id'] = worker_id
                    self.handle_sink(msg)
                    if 'last_msg' in self.worker_map.get(worker_id, {}):
                        del self.worker_map[worker_id]['last_msg']
                    self.msg_count += 1
                if socks.get(self.ventilator) & zmq.POLLOUT:
                    while self.outgoing_messages:
                        # find a worker that is free
                        worker_id = self.find_free_worker()
                        if not worker_id:
                            break

                        msg = self.outgoing_messages.pop()
                        # Send a calc message to the workers on the ventilator for the number of shards for this filename
                        # TODO Add a tracking of which requests have been sent out to the worker, and do retries with timeouts
                        self.worker_map[worker_id]['last_msg'] = time.time()
                        self.ventilator.send_multipart([worker_id, json.dumps(msg)])

                if socks.get(self.rpc) & zmq.POLLIN:
                    buf = self.rpc.recv_multipart()  # zmq.ROUTER sockets gets a triple with a msgid, blank msg and then the payload.
                    msg_id = binascii.hexlify(buf[0])
                    msg = json.loads(buf[2])
                    msg['token'] = msg_id
                    logger.debug('RPC received %s' % msg_id)
                    msg = msg_factory(msg)
                    self.handle_rpc(msg)
                if socks.get(self.rpc) & zmq.POLLOUT:
                    self.process_sink_results()

            except KeyboardInterrupt:
                logger.debug('Stopped from keyboard')
                self.kill()
            except:
                logger.error("Exception %s" % traceback.format_exc())

        logger.debug('Stopping')

    def kill(self):
        # Send a kill message to each of our workers
        for x in self.worker_map:
            self.ventilator.send_multipart([x, Message({'payload': 'kill', 'token': 'kill'}).to_json()])
        self.running = False

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
                    logger.debug('Errormesssage %s' % msg.get('payload'))
                    # Delete this entire message segments, if it still exists
                    if parent_token in self.rpc_segments:
                        del self.rpc_segments[parent_token]
                        # If any of the segment workers return an error,
                        # Send the exception on to the calling RPC
                        msg['token'] = parent_token
                        msg_id = binascii.unhexlify(parent_token)
                        tmp = [msg_id, '', json.dumps(msg)]
                        self.rpc.send_multipart(tmp)
                    # and continue the processing
                    continue

                params = msg.get_from_binary('params') or {}
                filename = params['args'][0]
                original_rpc['results'][filename] = msg.get_from_binary('result')

                if len(original_rpc['results']) == len(original_rpc['filenames']):
                    # Check to see that there are no filenames with no Result yet
                    # TODO as soon as any workers gives an error abort the whole enchilada

                    # if finished, aggregate the result
                    new_result = pd.concat(original_rpc['results'].values(), ignore_index=True)

                    if params.get('kwargs', {}).get('aggregate', True):
                        groupby_cols = params['args'][1]
                        measure_cols = params['args'][2]
                        if not groupby_cols:
                            new_result = pd.DataFrame(new_result.sum()).transpose()
                        else:
                            # aggregate over totals if needed
                            new_result = new_result.groupby(groupby_cols, as_index=False)[measure_cols].sum()

                    # We have received all the segment, send a reply to RPC caller
                    msg = original_rpc['msg']
                    msg.add_as_binary('result', new_result)
                    del self.rpc_segments[parent_token]
                else:
                    # This was a segment result move on
                    continue

            msg_id = binascii.unhexlify(msg.get('token'))
            tmp = [msg_id, '', json.dumps(msg)]
            self.rpc.send_multipart(tmp)
            logger.debug('Msg handled %s' % msg.get('token', '?'))

    def handle_rpc(self, msg):
        # Every msg needs a token, otherwise we don't know wo the reply goes to
        if 'token' not in msg:
            raise Exception('Every msg needs a token')

        # What kind of rpc calls can be made?
        data = {}
        if msg['payload'] == 'info':
            data = {'msg_count': self.msg_count,
                    'workers': self.worker_map
                    }
        elif msg['payload'] == 'kill':
            # And reply to our caller, otherwise they will just hang
            msg.add_as_binary('result', 'OK')
            self.rpc_results.append(msg)
            self.kill()
        elif msg['payload'] in ('groupby', 'sleep'):
            data = self.handle_calc_message(msg)
        else:
            data = "Sorry, I don't understand you"

        if data:
            msg.add_as_binary('result', data)
            self.rpc_results.append(msg)

    def handle_calc_message(self, msg):
        # Store the original message in the buf
        params = msg.get_from_binary('params') or {}

        if len(params.get('args', [])) != 4:
            return 'Error, No correct args given, expecting: ' + \
                   'path_list, groupby_col_list, measure_col_list, where_terms_list'

        filenames = params['args'][0]
        if not filenames:
            return 'Error, no filenames given'

        # Make sure that all filenames are available before any messages are sent
        for filename in filenames:
            if filename and filename not in self.files_map:
                return 'Sorry, filename %s was not found' % filename



        parent_token = msg['token']
        rpc_segment = {'msg': msg_factory(msg.copy()),
                       'results' : {},
                       'filenames': dict([(x, None) for x in filenames])}

        for filename in filenames:
            params['args'] = list(params['args'])
            params['args'][0] = filename
            msg.add_as_binary('params', params)

            # Make up a new token for the message sent to the workers, and collect the responses using that id
            msg['parent_token'] = parent_token
            new_token = binascii.hexlify(os.urandom(8))
            msg['token'] = new_token
            rpc_segment['filenames'][filename] = new_token
            self.outgoing_messages.append(msg.copy())

        self.rpc_segments[parent_token] = rpc_segment