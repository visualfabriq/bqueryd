'''Pulls in simple bquery commands, computes it, and pushes the result to a sink'''

import sys
import os
import time
import zmq
import config
import logging
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)
import bquery
import binascii
import traceback
import json
import cPickle
import random
import pandas as pd

def msg_factory(msg):
    if type(msg) is str:
        msg = json.loads(msg)
    if not msg:
        return Message()
    msg_mapping = {'calc': CalcMessage, 'rpc': RPCMessage, 'error': ErrorMessage,
                   'worker_register': WorkerRegisterMessage, None: Message }
    msg_class = msg_mapping.get(msg.get('msg_type'))
    return msg_class(msg)

class MalformedMessage(Exception):
    pass

class Message(dict):
    msg_type = None
    def __init__(self, datadict={}):
        if datadict is None:
            datadict = {}
        self.update(datadict)
        self['payload'] = datadict.get('payload')
        self['version'] = datadict.get('version', 1)
        self['msg_type'] = self.msg_type
    def add_as_binary(self, key, value):
        self[key] = cPickle.dumps(value).encode('base64')
    def get_from_binary(self, key):
        buf = self.get(key)
        if not buf: return
        return cPickle.loads(buf.decode('base64'))
    def to_json(self):
        return json.dumps(self)

class WorkerRegisterMessage(Message):
    msg_type = 'worker_register'
    def __init__(self, *args, **kwargs):
        super(WorkerRegisterMessage, self).__init__(*args, **kwargs)
        self['worker_id'] = binascii.hexlify(os.urandom(8))

class CalcMessage(Message):
    msg_type = 'calc'

class RPCMessage(Message):
    msg_type = 'rpc'

class ErrorMessage(Message):
    msg_type = 'error'


class WorkerNode(object):
    def __init__(self):
        self.data_dir = config.get('data_dir', os.getcwd())
        self.data_files = [filename for filename in os.listdir(self.data_dir) if filename.endswith(config.DATA_FILE_EXTENSION)]
        if len(self.data_files) < 1:
            logging.debug('Data directory %s has no files like %s' % (self.data_dir, config.DATA_FILE_EXTENSION))

        self.context = zmq.Context()
        wrm = WorkerRegisterMessage()
        self.worker_id = wrm['worker_id']
        wrm['data_files'] = self.data_files
        wrm['data_dir'] = self.data_dir
        # We receive operations to perform from the ventilator on the controller socket
        self.controller = self.context.socket(zmq.DEALER)
        self.controller.identity = self.worker_id
        # for now the ventilator is on localhost too, TODO get an address for it from central config
        self.controller.connect('tcp://127.0.0.1:%s' % config.VENTILATOR_PORT)
        self.controller.send_json(wrm)

    def go(self):
        logging.debug('WorkerNode started')

        self.running = True
        while self.running:
            time.sleep(0.0001) # give the system a breather to stop CPU usage being pegged at 100%
            msg = self.controller.recv_json()
            msg = msg_factory(msg)
            logging.debug('Worker %s Msg received %s' % (self.worker_id, msg))
            try:
                tmp = self.handle(msg)
            except Exception, e:
                tmp = ErrorMessage(msg)
                tmp['payload'] = traceback.format_exc()
            self.controller.send_json(tmp)
            # TODO send a periodic heartbeat to the controller, in case of transience
        logging.debug('Stopping Worker %s' % self.worker_id)

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
            filename = kwargs.pop('_filename')
            rootdir = os.path.join(self.data_dir, filename)
            if not os.path.exists(rootdir):
                msg['payload'] = 'Path %s does not exist' % rootdir
                return ErrorMessage(msg)
            ct = bquery.ctable(rootdir=rootdir)

            result_ctable = ct.groupby(*args)
            buf = result_ctable.todataframe()

        msg.add_as_binary('result', buf)
        return msg


class QNode(object):
    def __init__(self):
        self.context = zmq.Context()
        self.rpc = self.context.socket(zmq.ROUTER)
        rpc_address = 'tcp://*:%s' % config.RPC_PORT
        self.rpc.bind(rpc_address)
        logging.debug('RPC on %s' % rpc_address)

        self.ventilator = self.context.socket(zmq.ROUTER)
        ventilator_address = 'tcp://*:%s' % config.VENTILATOR_PORT
        self.ventilator.bind(ventilator_address)
        logging.debug('Ventilator on %s' % ventilator_address)

        self.msg_count = 0
        self.rpc_results = [] # buffer of results that are ready to be returned to callers
        self.rpc_segments = {} # Certain RPC calls get split and divided over workers, this dict tracks the original RPCs
        self.worker_map = {}  # maintain a list of connected workers TODO get rid of unresponsive ones...
        self.files_map = {} # shows on which workers a file is available on

    def handle_sink(self, msg):
        if isinstance(msg, WorkerRegisterMessage):
            tmp = {'last_seen': time.time(), 'wrm':msg}
            worker_id = msg.get('worker_id')
            self.worker_map[worker_id] = tmp
            logging.debug('QNode Worker registered %s' % worker_id)
            for filename in msg.get('data_files', []):
                self.files_map.setdefault(filename, set()).add(worker_id)
            return

        # Every msg needs a token, otherwise we don't know who the reply goes to
        if 'token' not in msg:
            logging.error('Every msg needs a token')
            return
        logging.debug('QNode Sink received %s' % msg.get('token', '?'))
        self.rpc_results.append(msg)

    def go(self):
        self.poller = zmq.Poller()
        self.poller.register(self.ventilator, zmq.POLLIN|zmq.POLLOUT)
        self.poller.register(self.rpc, zmq.POLLIN|zmq.POLLOUT)

        logging.debug('QNode started')

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
                    self.msg_count += 1

                if socks.get(self.rpc) & zmq.POLLIN:
                    buf = self.rpc.recv_multipart() # zmq.ROUTER sockets gets a triple with a msgid, blank msg and then the payload.
                    msg_id = binascii.hexlify(buf[0])
                    msg = json.loads(buf[2])
                    msg['token'] = msg_id
                    logging.debug('QNode Msg received %s' % msg)
                    msg = msg_factory(msg)
                    self.handle_rpc(msg)
                if socks.get(self.rpc) & zmq.POLLOUT:
                    self.process_sink_results()

            except KeyboardInterrupt:
                logging.debug('Stopped from keyboard')
                self.kill()
            except:
                logging.error("QNode Exception %s" % traceback.format_exc())

        logging.debug('Stopping QNode')

    def kill(self):
        # Send a kill message to each of our workers
        for x in self.worker_map:
            self.ventilator.send_multipart([x, Message({'payload': 'kill'}).to_json()] )
        self.running = False

    def process_sink_results(self):
        while self.rpc_results:
            msg = self.rpc_results.pop()
            # If this was a message to be combined, there should be a parent_token
            if 'parent_token' in msg:
                parent_token = msg['parent_token']
                original_rpc = self.rpc_segments.get(parent_token)

                params = msg.get_from_binary('params') or {}
                filename = params['kwargs']['_filename']
                original_rpc['filenames'][filename] = msg.get_from_binary('result')

                if len([True for v in original_rpc['filenames'].values() if v is None]) < 1: # Check to see that there are no filenames with no Result yet
                    new_result = pd.concat(original_rpc['filenames'].values(), ignore_index=True)
                    groupby_cols, agg_list = params['args']
                    new_result = new_result.groupby(groupby_cols)[agg_list] # TODO this needs to be generic and maybe done in a worker due to memory issues?

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
            logging.debug('QNode Msg handled %s' % msg.get('token', '?'))

    def handle_rpc(self, msg):
        # Every msg needs a token, otherwise we don't know wo the reply goes to
        if 'token' not in msg:
            raise Exception('Every msg needs a token')

        # What kind of rpc calls can be made?
        data = {}
        if msg['payload'] == 'info':
            data ={'msg_count': self.msg_count,
                    'workers': self.worker_map
                  }
        elif msg['payload'] == 'kill':
            # And reply to our caller, otherwise they will just hang
            msg.add_as_binary('result', 'OK')
            self.rpc_results.append(msg)
            self.kill()
        elif msg['payload'] in ('groupby', 'where_terms', 'sleep'):
            data = self.handle_calc_message(msg)
        else:
            data = "Sorry, I don't understand you"

        if data:
            msg.add_as_binary('result', data)
            self.rpc_results.append(msg)

    def handle_calc_message(self, msg):
        # Store the original message in the buf


        params = msg.get_from_binary('params') or {}
        filenames = params.get('kwargs', {}).get('_filenames', [])
        if not filenames:
            return 'Error, kwargs in msg has no _filenames parameter?'

        parent_token = msg['token']
        self.rpc_segments[parent_token] = {'msg': msg_factory(msg.copy()),
                                           'filenames': dict([(x,None) for x in filenames]) }

        for filename in filenames:
            if filename and filename not in self.files_map:
                return 'Sorry, filename %s was not found' % filename

            params['kwargs']['_filename'] = filename
            msg.add_as_binary('params', params)

            # Make up a new token for the message sent to the workers, and collect the responses using that id
            msg['parent_token'] = parent_token
            msg['token'] = binascii.hexlify(os.urandom(8))

            # Pick a random worker_id to send a message to for now, TODO add some kind of load-balancing & affinity
            worker_id = random.choice(self.worker_map.keys())
            # Send a calc message to the workers on the ventilator for the number of shards for this filename
            # TODO Add a tracking of which requests have been sent out to the worker, and do retries with timeouts
            self.ventilator.send_multipart([worker_id, json.dumps(msg)])


class RPC(object):

    def __init__(self):
        self.context = zmq.Context()
        self.qnode = self.context.socket(zmq.REQ)
        rpc_address = 'tcp://127.0.0.1:%s' % config.RPC_PORT
        self.qnode.connect(rpc_address)
        logging.debug('Will do RPC on %s' % rpc_address)

    def __getattr__(self, name):
        def _rpc(*args, **kwargs):
            start_time = time.time()
            params = {}
            if args:
                params['args'] = args
            if kwargs:
                params['kwargs'] = kwargs
            # We do not want string args to be converted into unicode by the JSON machinery
            # bquery ctable does not like col names to be unicode for example
            msg = RPCMessage({'payload':name})
            msg.add_as_binary('params', params)
            self.qnode.send_json(msg)
            rep = msg_factory(self.qnode.recv_json())
            result = rep.get_from_binary('result')
            stop_time = time.time()
            self.last_call_duration = stop_time - start_time
            return result
        return _rpc
 
if __name__ == '__main__':
    if '-w' in sys.argv: # Worker
        w = WorkerNode()
        w.go()
    elif '-q' in sys.argv: # Qnode
        q = QNode()
        q.go()