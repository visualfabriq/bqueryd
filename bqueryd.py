'''Pulls in simple bquery commands, computes it, and pushes the result to a sink'''

import sys
import os
import zmq
import config
import logging
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.DEBUG)
import bquery
import random, time


def msg_factory(msg):
    if msg.get('msg_type') == 'calc_message':
        m = CalcMessage(msg)
    elif msg.get('msg_type') == 'rpc_message':
        m =  RPCMessage(msg)
    else:
        m = Message(msg)
    return m.validate()

class MalformedMessage(Exception):
    pass

class Message(dict):
    def __init__(self, datadict):
        for k,v in datadict.items():
            self[k] = v
        self['payload'] = datadict.get('payload')
        self['version'] = datadict.get('version', 1)
    def validate(self):
        return self        

class CalcMessage(Message):
    def validate(self):
        if not self.get('filename'):
            raise MalformedMessage('CalcMessage needs a filename')
        return self

class RPCMessage(Message):
    def __init__(self, datadict):
        super(RPCMessage, self).__init__(datadict)
        self['msg_type'] = 'rpc_message'        


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
            tmp = handle(msg)
            self.results.send_json(tmp)

    def handle(self, msg):
        if not isinstance(msg, CalcMessage):
            logging.error('We can only handle CalcMessage types') 
        filename = msg.get('filename')
        # Do some bcolz schizzle here... :-)
        # For now let's fake it with something trivial
        buf = [random.randint(0,100) for x in range(1000000)]

        return {'version':1, 'type':'fakeresult', 'filename':filename, 'result':buf}


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

    def handle(self, msg):
        pass

    def go(self):
        poller = zmq.Poller()
        poller.register(self.sink, zmq.POLLIN)
        poller.register(self.rpc, zmq.POLLIN)

        logging.debug('QNode started')

        socks = {}
        self.running = True
        while self.running:
            try:
                socks = dict(poller.poll())
            except KeyboardInterrupt:
                logging.debug('Stopped from keyboard')
                running = False

            if socks.get(self.sink) == zmq.POLLIN:
                msg = self.sink.recv_json()
                self.handle(msg)
                self.sink_msg_count += 1

            if socks.get(self.rpc) == zmq.POLLIN:
                msg = self.rpc.recv_json()
                logging.debug('Msg received %s' % msg)
                msg = msg_factory(msg)                
                reply = self.handle_rpc(msg)
                logging.debug('Msg handled %s' % reply)
                self.rpc.send_json(reply)
        logging.debug('Stopping QNode')

    def handle_rpc(self, msg):
        # What kind of rpc calls can be made?
        data = {}
        if msg['payload'] == 'info':
            data = {'sink_msg_count': self.sink_msg_count,
                    'data_dir': self.data_dir,
                    'data_files': self.data_files
                   }
        if msg['payload'] == 'kill':
            self.running = False
        return RPCMessage(data)

class BQueryD(object):

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
    else:
        logging.debug('Testing connection')
        bd = BQueryD()
        print bd.info()