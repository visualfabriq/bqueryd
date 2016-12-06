'''Pulls in simple bquery commands, computes it, and pushes the result to a sink'''

import sys
import os
import time
import zmq
import config
import logging
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.DEBUG)
import bquery
import binascii
import traceback

def msg_factory(msg):
    msg_mapping = {None: Message, 'calc': CalcMessage, 'rpc': RPCMessage, 'error': ErrorMessage}
    msg_class = msg_mapping.get(msg.get('msg_type'))
    return msg_class(msg)

class MalformedMessage(Exception):
    pass

class Message(dict):
    def __init__(self, datadict):
        if datadict is None:
            datadict = {}
        for k,v in datadict.items():
            self[k] = v
        self['payload'] = datadict.get('payload')
        self['version'] = datadict.get('version', 1)

class CalcMessage(Message):
    def __init__(self, msg):
        super(CalcMessage, self).__init__(msg)
        self['msg_type'] = 'calc'

class RPCMessage(Message):
    def __init__(self, datadict):
        super(RPCMessage, self).__init__(datadict)
        self['msg_type'] = 'rpc'

class ErrorMessage(Message):
    @staticmethod
    def create(error):
        return ErrorMessage({'payload':error})
    def __init__(self, msg):
        super(ErrorMessage, self).__init__(msg)
        self['msg_type'] = 'error'

class BaseNode(object):
    def __init__(self):
        self.context = zmq.Context()

        self.data_dir = config.get('data_dir', os.getcwd())
        self.data_files = [filename for filename in os.listdir(self.data_dir) if filename.endswith(config.DATA_FILE_EXTENSION)]
        if len(self.data_files) < 1:
            logging.info('Data directory %s has no files like %s' % (self.data_dir, config.DATA_FILE_EXTENSION))    

class WorkerNode(BaseNode):
    def __init__(self):
        super(WorkerNode, self).__init__()
        # We receive operations to perform from the ventilator on the receive socket
        self.receive = self.context.socket(zmq.PULL)
        # for now the ventilator is on localhost too, TODO get an address for it from central config
        self.receive.connect('tcp://127.0.0.1:%s' % config.VENTILATOR_PORT)
        self.results = self.context.socket(zmq.PUSH)
        self.results.connect('tcp://127.0.0.1:%s' % config.SINK_PORT)

    def go(self):
        logging.debug('WorkerNode started')

        running = True
        while running:
            msg = self.receive.recv_json()
            msg = msg_factory(msg)
            logging.debug('Worker Msg received %s' % msg)
            try:
                tmp = self.handle(msg)
            except Exception, e:
                tmp = ErrorMessage(msg)
                tmp['payload'] = traceback.format_exc()
            self.results.send_json(tmp)

    def handle(self, msg):
        kwargs = msg.get('kwargs', {})
        filename = kwargs.get('filename')
        groupby_cols = kwargs.get('groupby_cols')
        agg_list = kwargs.get('agg_list')
        rootdir = os.path.join(self.data_dir, filename)
        if not os.path.exists(rootdir):
            return ErrorMessage.create('Path %s does not exist' % rootdir)
        ct = bquery.ctable(rootdir=rootdir)
        result_ctable = ct.groupby(groupby_cols, agg_list)
        result_dataframe = result_ctable.todataframe()
        buf = result_dataframe.to_msgpack()
        msg['result'] = buf

        return msg


class QNode(BaseNode):
    def __init__(self):
        super(QNode, self).__init__()

        self.rpc = self.context.socket(zmq.REP)
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
        self.rpc_map_ids = {} # keyed on msg_ids, values are the sockets and start times of the rpc calls
        self.rpc_map_soc = {} # keyed on socket, values are the msg_ids

    def handle_sink(self, msg):
        logging.debug('QNode Sink received %s' % msg)

    def go(self):
        poller = zmq.Poller()
        poller.register(self.sink, zmq.POLLIN)
        poller.register(self.rpc, zmq.POLLIN|zmq.POLLOUT)

        logging.debug('QNode started')

        socks = {}
        self.running = True
        while self.running:
            try:
                socks = dict(poller.poll())
            except KeyboardInterrupt:
                logging.debug('Stopped from keyboard')
                self.running = False

            if socks.get(self.sink) == zmq.POLLIN:
                msg = self.sink.recv_json()
                self.handle_sink(msg)
                self.sink_msg_count += 1

            if socks.get(self.rpc) == zmq.POLLIN:
                msg = self.rpc.recv_json()
                msg_id = binascii.hexlify(os.urandom(8))
                self.rpc_map_ids[msg_id] = {'socket': self.rpc, 'rcv_time':time.time()}
                self.rpc_map_soc[self.rpc] = msg_id
                msg['token'] = msg_id
                logging.debug('QNode Msg received %s' % msg)
                msg = msg_factory(msg)                
                self.handle_rpc(msg)
            if socks.get(self.rpc) == zmq.POLLOUT:
                msg_id = self.rpc_map_soc.get(self.rpc)
                reply = self.rpc_map_ids.get(msg_id).get('result')
                if reply:
                    logging.debug('QNode Msg handled %s' % reply)
                    self.rpc.send_json(reply)

        logging.debug('Stopping QNode')

    def handle_rpc(self, msg):
        # Every msg needs a token, otherwise we don't know wo the reply goes to
        if 'token' not in msg:
            raise Exception('Every msg needs a token')

        # What kind of rpc calls can be made?
        data = {}
        if msg['payload'] == 'info':
            data = {'sink_msg_count': self.sink_msg_count,
                    'data_dir': self.data_dir,
                    'data_files': self.data_files
                   }
        elif msg['payload'] == 'kill':
            self.running = False
        elif msg['payload'] == 'calc':
            data = self.handle_calc(msg)
        else:
            data = {'payload': "Sorry, I don't understand you"}

        tmp = self.rpc_map_ids.get(msg['token'])
        if not tmp:
            logging.debug('Error: RPC token %s not found in rpc_map_ids' % msg['token'])
            return
        tmp['result'] = RPCMessage(data)

    def handle_calc(self, msg):
        # Send a calc message to the workers on the ventilatior for the number of shards for this filename
        self.ventilator.send_json(msg)

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
