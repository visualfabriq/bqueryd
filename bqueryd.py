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

def msg_factory(msg):
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

class WorkerRegisterMessage(Message):
    msg_type = 'worker_register'
    def __init__(self, *args):
        super(WorkerRegisterMessage, self).__init__()
        self['worker_id'] = binascii.hexlify(os.urandom(8))

class CalcMessage(Message):
    msg_type = 'calc'

class RPCMessage(Message):
    msg_type = 'rpc'

class ErrorMessage(Message):
    msg_type = 'error'

class BaseNode(object):
    def __init__(self):
        self.context = zmq.Context()

        self.data_dir = config.get('data_dir', os.getcwd())
        self.data_files = [filename for filename in os.listdir(self.data_dir) if filename.endswith(config.DATA_FILE_EXTENSION)]
        if len(self.data_files) < 1:
            logging.debug('Data directory %s has no files like %s' % (self.data_dir, config.DATA_FILE_EXTENSION))

class WorkerNode(BaseNode):
    def __init__(self):
        super(WorkerNode, self).__init__()
        # We receive operations to perform from the ventilator on the receive socket
        self.receive = self.context.socket(zmq.PULL)
        # for now the ventilator is on localhost too, TODO get an address for it from central config
        self.receive.connect('tcp://127.0.0.1:%s' % config.VENTILATOR_PORT)
        self.results = self.context.socket(zmq.PUSH)
        self.results.connect('tcp://127.0.0.1:%s' % config.SINK_PORT)
        wrm = WorkerRegisterMessage()
        self.worker_id = wrm['worker_id']
        self.results.send_json(wrm)

    def go(self):
        logging.debug('WorkerNode started')

        self.running = True
        while self.running:
            msg = self.receive.recv_json()
            msg = msg_factory(msg)
            logging.debug('Worker Msg received %s' % msg)
            try:
                tmp = self.handle(msg)
            except Exception, e:
                tmp = ErrorMessage(msg)
                tmp['payload'] = traceback.format_exc()
            self.results.send_json(tmp)
        logging.debug('Stopping Worker %s' % self.worker_id)

    def handle(self, msg):
        kwargs = msg.get('kwargs', {})
        args = msg.get('args', [])

        if msg.get('payload') == 'kill':
            self.running = False
            return
        elif msg.get('payload') == 'sleep':
            time.sleep(float(args[0]))
            buf = 'zzzzz'
        else:
            filename = kwargs.pop('_filename')
            groupby_cols = kwargs.get('groupby_cols')
            agg_list = kwargs.get('agg_list')

            rootdir = os.path.join(self.data_dir, filename)
            if not os.path.exists(rootdir):
                msg['payload'] = 'Path %s does not exist' % rootdir
                return ErrorMessage(msg)
            ct = bquery.ctable(rootdir=rootdir)
            result_ctable = ct.groupby(*args)
            result_dataframe = result_ctable.todataframe()
            buf = result_dataframe.to_msgpack()

        msg['result'] = buf
        return msg


class QNode(BaseNode):
    def __init__(self):
        super(QNode, self).__init__()

        self.rpc = self.context.socket(zmq.ROUTER)
        rpc_address = 'tcp://*:%s' % config.RPC_PORT
        self.rpc.bind(rpc_address)
        logging.debug('RPC on %s' % rpc_address)

        self.ventilator = self.context.socket(zmq.PUSH)
        ventilator_address = 'tcp://*:%s' % config.VENTILATOR_PORT
        self.ventilator.bind(ventilator_address)
        logging.debug('Ventilator on %s' % ventilator_address)

        self.sink = self.context.socket(zmq.PULL)
        sink_address = 'tcp://*:%s' % config.SINK_PORT
        self.sink.bind(sink_address)
        logging.debug('Sink on %s' % sink_address)

        self.sink_msg_count = 0
        self.rpc_results = [] # buffer of results that are ready to be returned to callers
        self.worker_map = {}  # maintain a list of connected workers TODO get rid of unresponsive ones...

    def handle_sink(self, msg):
        logging.debug('QNode Sink received %s' % msg)
        if isinstance(msg, WorkerRegisterMessage):
            self.worker_map[msg['worker_id']] = {'last_seen': time.time()}
            return

        # Every msg needs a token, otherwise we don't know who the reply goes to
        if 'token' not in msg:
            raise Exception('Every msg needs a token')

        self.rpc_results.append(msg)

    def go(self):
        self.poller = zmq.Poller()
        self.poller.register(self.sink, zmq.POLLIN)
        self.poller.register(self.rpc, zmq.POLLIN|zmq.POLLOUT)

        logging.debug('QNode started')

        socks = {}
        self.running = True
        while self.running:
            try:
                socks = dict(self.poller.poll())

                if socks.get(self.sink) == zmq.POLLIN:
                    msg = self.sink.recv_json()
                    msg = msg_factory(msg)
                    self.handle_sink(msg)
                    self.sink_msg_count += 1

                if socks.get(self.rpc) & zmq.POLLIN:
                    buf = self.rpc.recv_multipart() # zmq.ROUTER sockets gets a triple with a msgid, blank msg and then the payload.
                    msg_id = binascii.hexlify(buf[0])
                    msg = json.loads(buf[2])
                    msg['token'] = msg_id
                    logging.debug('QNode Msg received %s' % msg)
                    msg = msg_factory(msg)
                    self.handle_rpc(msg)
                if socks.get(self.rpc) & zmq.POLLOUT:
                    while self.rpc_results:
                        msg = self.rpc_results.pop()
                        msg_id = binascii.unhexlify(msg.get('token'))
                        tmp = [msg_id, '', json.dumps(msg)]
                        self.rpc.send_multipart(tmp)
                        logging.debug('QNode Msg handled %s' % msg)

            except KeyboardInterrupt:
                logging.debug('Stopped from keyboard')
                self.kill()
            except:
                logging.error("QNode Exception %s" % traceback.format_exc())

        logging.debug('Stopping QNode')

    def kill(self):
        # Also kill the workers, we don't have direct connections to them
        # but let's send 2 * count of workers to the ventilator...
        for x in range(len(self.worker_map)):
            self.ventilator.send_json(Message({'payload': 'kill'}))
        self.running = False

    def handle_rpc(self, msg):
        # Every msg needs a token, otherwise we don't know wo the reply goes to
        if 'token' not in msg:
            raise Exception('Every msg needs a token')

        # What kind of rpc calls can be made?
        data = {}
        if msg['payload'] == 'info':
            data = {'sink_msg_count': self.sink_msg_count,
                    'data_dir': self.data_dir,
                    'data_files': self.data_files,
                    'workers': self.worker_map
                   }
        elif msg['payload'] == 'kill':
            # And reply to our caller, otherwise they will just hang
            self.rpc.send_json(RPCMessage({'payload': 'OK'}))
            self.kill()
        elif msg['payload'] in ('groupby', 'where_terms', 'sleep'):
            # Send a calc message to the workers on the ventilator for the number of shards for this filename
            # TODO Add a tracking of which requests have been sent out to the worker, and do retries with timeouts
            self.ventilator.send_json(msg)
        else:
            data = {'payload': "Sorry, I don't understand you"}

        if data:
            msg.update(data)
            self.rpc_results.append(msg)

class RPC(object):

    def __init__(self):
        self.context = zmq.Context()
        self.qnode = self.context.socket(zmq.REQ)
        rpc_address = 'tcp://127.0.0.1:%s' % config.RPC_PORT
        self.qnode.connect(rpc_address)
        logging.debug('Will do RPC on %s' % rpc_address)

    def __getattr__(self, name):
        def _rpc(*args, **kwargs):
            tmp = {'payload':name}
            if args:
                tmp['args'] = args
            if kwargs:
                tmp['kwargs'] = kwargs
            print RPCMessage(tmp)

            self.qnode.send_json(RPCMessage(tmp))
            rep = self.qnode.recv_json()
            return rep
        return _rpc
 
if __name__ == '__main__':
    if '-w' in sys.argv: # Worker
        w = WorkerNode()
        w.go()
    elif '-q' in sys.argv: # Qnode
        q = QNode()
        q.go()
